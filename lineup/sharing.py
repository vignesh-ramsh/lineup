"""Idle workers helping other queues.

A worker normally runs only the jobs of the queue(s) it was started for (`arc lineup worker
--queues critical`), so while `default` has a backlog a worker of `critical` sits idle. With the
setting `lineup_share_idle_workers` switched on, a worker whose own ("home") queue has nothing to
do takes a job from another queue and runs it. Everything here is about doing that without
breaking what the separate queues are for:

  * The home queue always comes first. A worker looks at another queue only when its home queue
    has no backlog and a job slot is free (a slot that a home job is waiting for is never taken).
  * Borrowed jobs share the worker's own job slots (`lineup_concurrency`, by default the size of
    its database pool), so a worker never runs more jobs, or holds more database connections, than
    it does without sharing.
  * At least one slot per home queue is never lent out, so a worker that is busy with borrowed
    jobs can still start a job of its own queue at once. The rest can be lent, so a worker needs
    two or more job slots before it borrows anything.
  * A queue named in `lineup_strict_queues` is left alone in both directions: nobody borrows from
    it, and a worker that has it as a home queue does not borrow either.
  * A borrowed job is taken only when a slot is free, right before it starts, so no job is ever
    held back in the worker's memory waiting for a slot.

The setting is read every second, so switching it takes effect without restarting the workers: a
job already running finishes normally, and no new one is borrowed.

A running job cannot be interrupted. A worker that borrowed a long job is busy with it until it
ends, which is why queues with long or slow jobs belong in `lineup_strict_queues`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections.abc import Iterable
from typing import Any

from redis.asyncio import Redis
from taskiq.receiver import Receiver

import arc

SHARE_KEY = "lineup_share_idle_workers"
STRICT_KEY = "lineup_strict_queues"

#: A free slot, but nothing to borrow: look again after this long (each look costs a Redis call).
IDLE_POLL_SECONDS = 1.0
#: No free slot (or the home queue has a backlog): look again soon, this costs nothing.
BUSY_POLL_SECONDS = 0.25
#: How often to ask the database which queues have waiting jobs this worker has never heard of.
DISCOVERY_SECONDS = 30.0

logger = logging.getLogger("lineup.sharing")


# --------------------------------------------------------------------------- #
# The settings
# --------------------------------------------------------------------------- #
def declare_settings(kernel: Any) -> None:
    kernel.settings.declare(
        SHARE_KEY,
        type=bool,
        default=False,
        doc="Let a worker that has nothing to do in its own queue run jobs of other queues. Off (the default): "
        "every worker runs only the queue it was started for. On: an idle worker helps whichever queue has a "
        "backlog, using the job slots it already has, always looking at its own queue first and always keeping "
        "one slot free for it. A job that is running cannot be interrupted, so a worker that borrowed a long "
        "job is busy until it ends: turn this on when your jobs are short, and list queues with long or "
        "sensitive jobs in `lineup_strict_queues`. Takes effect within a few seconds, no restart needed.",
    )
    kernel.settings.declare(
        STRICT_KEY,
        type=str,
        default="",
        doc="Queues that stay strictly separate even when `lineup_share_idle_workers` is on, as a comma-separated "
        "list (for example `mail,filer`). Workers of other queues never take jobs from a queue listed here, and "
        "the workers of a listed queue never take jobs from other queues. Use it for a queue whose throughput is "
        "limited on purpose (an email provider's rate limit) or whose jobs are slow or heavy.",
    )


def sharing_enabled() -> bool:
    return arc.settings.get(SHARE_KEY) is True


def parse_queue_list(raw: object) -> list[str]:
    """`"mail, filer"` / `"mail filer"` -> ["mail", "filer"]; blanks and repeats dropped."""
    if not raw:
        return []
    return list(dict.fromkeys(part for part in re.split(r"[,\s]+", str(raw)) if part))


def strict_queues() -> frozenset[str]:
    return frozenset(parse_queue_list(arc.settings.get(STRICT_KEY)))


def guest_queue_names(*, home: Iterable[str], known: Iterable[str], strict: Iterable[str]) -> list[str]:
    """The queues a worker may borrow from, in the order it looks at them (the order `known` has):
    not its own, and not a strict one. A worker with a strict home queue borrows from nobody."""
    home_set, strict_set = set(home), set(strict)
    if home_set & strict_set:
        return []
    return [q for q in dict.fromkeys(known) if q not in home_set and q not in strict_set]


# --------------------------------------------------------------------------- #
# The job slots a worker can lend
# --------------------------------------------------------------------------- #
class _Lane:
    """The job slots of one home-queue listener: its semaphore, and how many are lent out."""

    __slots__ = ("capacity", "lent", "queue", "sem")

    def __init__(self, queue: str, sem: asyncio.Semaphore, capacity: int) -> None:
        self.queue = queue
        self.sem = sem
        self.capacity = capacity
        self.lent = 0


class Slot:
    """One lent job slot. `release()` gives it back, once; releasing again does nothing."""

    __slots__ = ("_lane", "_sem")

    def __init__(self, lane: _Lane) -> None:
        self._lane: _Lane | None = lane
        self._sem = lane.sem  # the semaphore it came from, even if that listener has restarted since

    def release(self) -> None:
        lane, self._lane = self._lane, None
        if lane is None:
            return
        lane.lent -= 1
        self._sem.release()


class HomeSlots:
    """The job slots of the home-queue listeners currently running in this process. A listener
    registers its taskiq semaphore here while it listens (`attach`) and withdraws it when it
    stops (`detach`); `borrow()` takes one slot out of a semaphore that has one free."""

    def __init__(self) -> None:
        self._lanes: dict[str, _Lane] = {}

    def attach(self, queue: str, sem: asyncio.Semaphore, capacity: int) -> None:
        self._lanes[queue] = _Lane(queue, sem, capacity)

    def detach(self, queue: str, sem: asyncio.Semaphore) -> None:
        lane = self._lanes.get(queue)
        if lane is not None and lane.sem is sem:  # a restarted listener has already replaced it
            del self._lanes[queue]

    def has_slots(self) -> bool:
        return bool(self._lanes)

    async def borrow(self) -> Slot | None:
        """A slot nobody else wants right now, or None. Never the last slot of a listener, and
        never one a home job is already waiting for."""
        for lane in self._lanes.values():
            if lane.lent >= lane.capacity - 1 or lane.sem.locked():
                continue
            await lane.sem.acquire()  # not locked: returns at once, nothing can slip in between
            lane.lent += 1
            return Slot(lane)
        return None


# --------------------------------------------------------------------------- #
# The borrower
# --------------------------------------------------------------------------- #
class IdleBorrower:
    """Runs next to a worker's own listeners (`run()`), looking for jobs in other queues
    whenever a job slot is free. See the module docstring for the rules."""

    def __init__(self, *, home_queues: Iterable[str], slots: HomeSlots, shutdown_event: asyncio.Event) -> None:
        self._home = list(home_queues)
        self._slots = slots
        self._shutdown = shutdown_event
        self._started: set[str] = set()
        self._receivers: dict[str, Receiver] = {}
        self._running: set[asyncio.Task] = set()
        self._helped: set[str] = set()
        self._last_discovery: float | None = None
        self._warned_no_slots = False

    async def run(self) -> None:
        try:
            while not self._shutdown.is_set():
                try:
                    pause = await self._step()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - one bad pass must not end borrowing for good
                    logger.error("lineup: looking for jobs to borrow failed: %r", exc)
                    pause = IDLE_POLL_SECONDS * 5
                await self._pause(pause)
        finally:
            await self._drain()

    async def _pause(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._shutdown.wait(), seconds)

    async def _step(self) -> float:
        """One look around. Returns how long to wait before the next one."""
        if not sharing_enabled():
            return IDLE_POLL_SECONDS
        strict = strict_queues()
        if set(self._home) & strict:
            return IDLE_POLL_SECONDS  # this worker's own queue is strict: it never borrows
        if not self._slots.has_slots():
            if not self._warned_no_slots:
                self._warned_no_slots = True
                logger.warning(
                    "lineup: %s is on but this worker has no job-slot limit to share (no database pool "
                    "size and lineup_concurrency is 0): it will not borrow jobs",
                    SHARE_KEY,
                )
            return IDLE_POLL_SECONDS

        await self._discover_queues()
        guests = guest_queue_names(home=self._home, known=arc.lineup.broker_map(), strict=strict)
        if not guests:
            return IDLE_POLL_SECONDS

        slot = await self._slots.borrow()
        if slot is None:
            return BUSY_POLL_SECONDS
        try:
            if await self._home_has_backlog():
                slot.release()
                return BUSY_POLL_SECONDS
            found = await self._pop_first(guests)
        except BaseException:
            slot.release()
            raise
        if found is None:
            slot.release()
            return IDLE_POLL_SECONDS
        queue, raw = found
        self._launch(queue, raw, slot)
        return 0.0  # a queue with one waiting job often has more

    # -- finding work ------------------------------------------------------- #
    async def _discover_queues(self) -> None:
        """A queue created after this worker started (an ad hoc `enqueue(fn, queue="...")`) is not
        in its list: ask the database which queues have waiting jobs, now and then."""
        loop = asyncio.get_running_loop()
        now = loop.time()
        if self._last_discovery is not None and now - self._last_discovery < DISCOVERY_SECONDS:
            return
        self._last_discovery = now
        async with arc.pgdb.acquire() as conn:
            rows = await conn.fetch(
                'SELECT DISTINCT queue FROM "_job_log" WHERE status = $1 AND executor = $2 LIMIT 200',
                "Queued",
                "lineup",
            )
        for row in rows:
            arc.lineup.ensure_queue(row["queue"])

    async def _home_has_backlog(self) -> bool:
        brokers = arc.lineup.broker_map()
        for queue in self._home:
            broker = brokers.get(queue)
            if broker is None:
                continue
            async with Redis(connection_pool=broker.connection_pool) as conn:
                if await conn.llen(broker.queue_name):
                    return True
        return False

    async def _pop_first(self, guests: list[str]) -> tuple[str, bytes] | None:
        brokers = arc.lineup.broker_map()
        for queue in guests:
            broker = brokers[queue]
            await self._ensure_started(queue, broker)
            async with Redis(connection_pool=broker.connection_pool) as conn:
                raw = await conn.rpop(broker.queue_name)
            if raw is not None:
                return queue, raw
        return None

    async def _ensure_started(self, queue: str, broker: Any) -> None:
        if queue not in self._started:
            broker.is_worker_process = True
            await broker.startup()
            self._started.add(queue)

    # -- running it --------------------------------------------------------- #
    def _receiver_for(self, queue: str) -> Receiver:
        receiver = self._receivers.get(queue)
        if receiver is None:
            # Only `callback()` is used, which has no slot limit of its own (the slot was taken from
            # the home listener); max_async_tasks=1 just keeps taskiq from warning about "unlimited".
            receiver = Receiver(broker=arc.lineup.broker_map()[queue], max_async_tasks=1, run_startup=False)
            self._receivers[queue] = receiver
        return receiver

    def _launch(self, queue: str, raw: bytes, slot: Slot) -> None:
        if queue not in self._helped:
            self._helped.add(queue)
            logger.info("lineup worker (home: %s) is idle and now helping queue %r", ", ".join(self._home), queue)
        task = asyncio.create_task(self._run_borrowed(queue, raw, slot))
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    async def _run_borrowed(self, queue: str, raw: bytes, slot: Slot) -> None:
        try:
            await self._receiver_for(queue).callback(message=raw, raise_err=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - taskiq reports a job's own failure itself; this is anything else
            logger.error("lineup: running a job borrowed from queue %r failed: %r", queue, exc)
            arc.errors.capture(exc, source="lineup", queue=queue, kind="Borrowed")
        finally:
            slot.release()

    async def _drain(self) -> None:
        """Shutting down: let the borrowed jobs finish (a home listener does the same), then close
        the brokers this borrower started."""
        if self._running:
            await asyncio.wait(set(self._running))
        brokers = arc.lineup.broker_map()
        for queue in self._started:
            broker = brokers.get(queue)
            if broker is not None:
                await broker.shutdown()
        self._started.clear()
