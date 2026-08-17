"""
lineup — ARC provider plugin: durable background jobs + cron-style
scheduling, built on TaskIQ with a Redis broker (docs/arc.MD §7 Phase 6.5
follow-up — replaces `arc.relay.enqueue()`'s in-process-only fallback with
a real durable backend, and adds a scheduler that didn't exist at all
before this).

Backend: whatever server `redix_url` points at — a real Redis, or Valkey
(the wire-compatible fork; see redix/__init__.py's own module docstring
for the full reasoning). `taskiq-redis`'s `ListQueueBroker`/`Receiver`,
like `redis-py`, is a RESP client — it neither knows nor cares which of
the two is listening on the other end of the socket. Every "Redis" below
describing the queue/broker's general behavior (the LIST it pops from,
what a worker crash does to an in-flight message) applies identically to
Valkey; literal identifiers (`ListQueueBroker`, `taskiq-redis`,
`redis.asyncio`) keep their real names either way, same as redix's own.

Business plugins are not meant to import this module or call `arc.lineup`
directly — `arc.relay.task(...)`/`arc.relay.register_tasks(...)`/
`arc.relay.enqueue(...)` are the intended surface (docs/arc.MD §3.15),
exactly the same posture already established for `redix` (a plugin calls
`arc.relay.cache_get/cache_set/lock`, never `arc.redix` itself). Relay
reaches for `arc.lineup` internally when it's installed; a plugin that
only ever goes through relay needs no dependency on lineup at all, and
its jobs keep working (just without durability/scheduling) if lineup is
ever removed.

requires=["redix"] — reuses redix's already-resolved connection URL
(kernel.get("redix").url) directly rather than declaring a second,
duplicate `lineup_redis_url` setting. Two independent RESP client
libraries end up talking to the same instance (redix's own `redis.asyncio`
client for cache/lock/pubsub, TaskIQ's own connection pool for the queue
lists) — that's fine, they don't share connections and don't need to; the
one thing that has to match is the URL itself, whichever server it's
actually pointed at.

Multiple named queues, not one global queue: `queue_name` on TaskIQ's own
`ListQueueBroker` maps directly to a distinct list key on that instance,
so "a different type of queue" is just a different string — `@arc.lineup.task
(queue="high")` and `@arc.lineup.task(queue="default")` are two completely
independent lists, consumed by whichever `arc lineup worker --queues=...`
processes choose to listen to them. No fixed enum of queue names is
enforced here — "default"/"high"/"low" below are a suggested convention,
not a hard requirement, same "business declares it, framework doesn't
gatekeep it" posture as SELECT field options elsewhere in this project.

Scheduling never fires at registration time. This was an explicit
requirement, not an incidental property: a scheduled job is a TaskIQ task
carrying a `schedule=[{"cron": "..."}]` label (TaskIQ's own
LabelScheduleSource mechanism) — `arc lineup scheduler` polls once a
second and only actually dispatches a task when the real wall-clock
minute matches the cron expression (taskiq.cli.scheduler.run.
is_cron_task_now, backed by `pycron.is_now`). Registering a job (calling
register_tasks() during boot) only ever adds an entry to that check — it
never calls the task itself. A job registered today with a nightly cron
correctly waits for tonight's occurrence (or tomorrow's, if tonight's has
already passed) — never runs the moment the process starts.

Two ways to get a durable job, for two different situations — neither
requires touching this module directly (docs/arc.MD §3.15, `arc.relay` is
the facade for both):
  * **Known ahead of time, or scheduled** — `@arc.relay.task(queue=...,
    cron=...)` in a plugins/<plugin>/tasks/*.py file. Pre-registered at
    boot, so a bad cron string or a duplicate name is a hard error before
    anything runs.
  * **Ad hoc, from anywhere** — `arc.relay.enqueue(some_plain_function,
    queue=..., ...)`, no decorator, no special directory, called from a
    whitelisted function, a hook, wherever. Nothing is pre-registered:
    `enqueue_by_path()` below sends only the function's own
    `(module, qualname)` over the wire (TaskIQ never sends code, only a
    name + arguments), and a worker re-imports the real function fresh when
    the job actually runs. The one real requirement this imposes — `fn`
    has to be a genuine plain, module-level function, not a lambda or a
    closure — is checked immediately, synchronously, at the call site
    (`check_resolvable()`), not discovered later inside a worker.

Call context crosses the process boundary with the job. A durable job runs
in a different process than the one that queued it, so relay's own
`CallContext` contextvar (docs/arc.MD §3.11 — request_id / user / roles)
cannot reach it. Instead the context travels as data: it is stamped onto
the TaskIQ message as `arc_ctx_*` labels at kick time and rebound around
the job on the worker side, so `arc.relay.context()` answers "which
request, which user" identically in both processes, and every `_job_log`
row records `request_id`/`triggered_by`. lineup deliberately treats those
labels as OPAQUE — relay owns both encode and decode (`context_labels()`/
`use_context_labels()`), which keeps the dependency direction right
(relay optionally depends on lineup, never the reverse) and means a new
context field needs no change in this module at all.

Known, deliberate limitation of the broker choice (ListQueueBroker, a
plain list via BRPOP): a message is removed from the list the
instant a worker pops it, before the task finishes running — so a durable
job now survives the *enqueuing* process (Gateway) crashing or restarting
before a worker ever picks it up (the original problem this plugin
exists to fix), but does NOT survive a *worker* crashing mid-task after
having already popped the job (see enqueue()/_reap_and_run_stale below for
how the durable-queue row now covers exactly that gap). taskiq-redis also
ships RedisStreamBroker
(consumer groups, ack/redelivery) for that stronger guarantee — not used
here, to keep the first version simple; swapping the broker class is a
contained, later change if a real need for it shows up (same
"don't build ahead of a real need" posture as everywhere else, docs/
arc.MD §7).
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import logging
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import pycron
from taskiq import Context, TaskiqDepends
from taskiq_redis import ListQueueBroker

import arc


def _local_cron_offset() -> str | timedelta:
    """TaskIQ's scheduler evaluates every cron expression against UTC by
    default (`taskiq.cli.scheduler.run.is_cron_task_now` builds `now =
    datetime.now(tz=timezone.utc)` and passes it straight to
    `pycron.is_now()` unless a `cron_offset` label says otherwise) — but
    every cron string declared through `task()` below is documented and
    intended to fire at the SERVER's own local wall-clock time (docs/
    arc.MD §3.15's own worked example: cron "15 18 * * *" annotated
    "local system time IST"), not UTC. Without this, a task registered
    with that cron would actually fire at 18:15 UTC (23:45 IST) instead.

    Resolved from /etc/localtime's own symlink target (e.g. .../zoneinfo/
    Asia/Kolkata -> "Asia/Kolkata") so this tracks whatever zone the
    server is actually configured with, rather than hardcoding one — a
    fixed UTC-offset timedelta (still correct for matching cron fields,
    just not DST-aware) is the fallback if that symlink is missing or
    doesn't resolve under a zoneinfo directory (e.g. non-Linux hosts)."""
    try:
        target = str(Path("/etc/localtime").resolve())
        marker = "zoneinfo/"
        idx = target.find(marker)
        if idx != -1:
            return target[idx + len(marker) :]
    except OSError:
        pass
    return datetime.now().astimezone().utcoffset() or timedelta(0)


CAPABILITY = "lineup"
DEFAULT_QUEUE = "default"
QUEUE_PREFIX = "lineup:"

# How long a claim on a _job_log row (claimed_by/lease_expires_at) is
# honored before another poller treats it as abandoned and re-claims it —
# same reasoning as relay/background_jobs.py's identical constant, applied
# here to lineup's own reaper (_reap_and_run_stale) instead of relay's.
_JOB_LEASE_SECONDS = 60
_REAP_INTERVAL_SECONDS = 15

logger = logging.getLogger("lineup")


class CronValueError(ValueError):
    """Raised at registration time for a malformed cron expression —
    failing fast here beats discovering it only once the scheduler process
    happens to poll it (docs/arc.MD's general "hard error before it gets
    weird" posture, e.g. psqldb's schema validation)."""


class LineupProvider:
    def __init__(self, kernel: Any, redis_url: str) -> None:
        self._kernel = kernel
        self._redis_url = redis_url
        self._brokers: dict[str, ListQueueBroker] = {}
        self._tasks: dict[str, Any] = {}
        self._task_queue: dict[str, str] = {}  # task_name -> queue, for enqueue()'s own Queued log row
        # task_name -> (module_path, qualname) of the ORIGINAL, undecorated
        # function a @task(...) wrapped — captured at registration time,
        # before wrapping, because the decorated object itself has no
        # stable importable path of its own. This is what lets a Queued
        # row for a registered task carry a `payload` a reaper can
        # reconstruct and run directly (_reap_and_run_stale), the same way
        # an ad hoc enqueue_by_path() job already can via check_resolvable.
        # A task whose fn doesn't resolve cleanly (a closure — unusual for
        # a module-level @task() declaration, but not impossible) just gets
        # no entry here: its payload stays None, exactly like an
        # enqueue_by_path() call the reaper can't reconstruct either.
        self._task_paths: dict[str, tuple[str, str]] = {}
        self._dispatch_tasks: dict[str, Any] = {}
        self._loading_plugin: str | None = None
        # Claim identity for this process's rows (_job_log.claimed_by) +
        # the reaper poll task started/stopped by open()/close().
        self._worker_id = f"lineup-{uuid.uuid4().hex[:12]}"
        self._reap_task: asyncio.Task | None = None
        # Lifecycle: open() starts every broker that exists at that moment,
        # but brokers are created LAZILY (_broker_for) — an ad hoc enqueue
        # to a brand-new queue name after startup creates one that open()
        # never saw. _opened/_started let enqueue_by_path() start such a
        # broker on first use instead of silently relying on taskiq-redis
        # happening to lazy-init its own pool.
        self._opened = False
        self._started: set[str] = set()

    # ------------------------------------------------------------------ #
    # Broker access — one ListQueueBroker per distinct queue name, created
    # lazily the first time anything touches that queue (a `task()`
    # declaration, an `enqueue_by_path()` call, or an operator explicitly
    # asking a worker/scheduler to listen on it, `ensure_queue()` below).
    #
    # Every NEW broker immediately gets its generic dispatch task
    # registered too (_register_dispatch_task) — not just when
    # enqueue_by_path() happens to be the thing that created it. This
    # matters for a real reason, found by testing: an ad hoc job's message
    # only names a task ("lineup._dispatch.<queue>"), never carries the
    # function itself, so a WORKER process — which does its own,
    # completely separate arc.boot() and only ever knows about a queue
    # because ITS OWN boot happened to touch it — needs that same
    # dispatch task registered in ITS OWN process before it can run
    # anything sent to that queue. Registering it unconditionally,
    # whenever any process creates that broker at all, guarantees every
    # process that boots and touches a queue (enqueuer, worker, or
    # scheduler) ends up with an identical registration, regardless of
    # which one happens to run first.
    # ------------------------------------------------------------------ #
    def _broker_for(self, queue: str) -> ListQueueBroker:
        if queue not in self._brokers:
            broker = ListQueueBroker(url=self._redis_url, queue_name=f"{QUEUE_PREFIX}{queue}")
            self._brokers[queue] = broker
            self._register_dispatch_task(broker, queue)
        return self._brokers[queue]

    def ensure_queue(self, queue: str) -> None:
        """Creates the broker (+ its dispatch task) for `queue` if nothing
        has touched it yet in this process — for a purely ad hoc queue
        name that no `@arc.relay.task(...)` anywhere ever declared (only
        ever used via `enqueue(fn, queue="...")` calls), nothing during a
        worker's own boot would otherwise create it, since boot only
        registers pre-declared tasks. `arc lineup worker --queues=...`
        calls this for every queue an operator explicitly asks it to
        listen on, exactly so that case works too."""
        self._broker_for(queue)

    def queues(self) -> list[str]:
        return sorted(self._brokers.keys())

    def broker_map(self) -> dict[str, ListQueueBroker]:
        return dict(self._brokers)

    def scheduled_tasks(self) -> list[dict]:
        """Every task currently carrying a cron schedule, across every
        queue — the exact same live config `arc lineup scheduler`'s own
        `LabelScheduleSource` reads from (§3.15), surfaced here purely for
        introspection (admin's Scheduled Jobs listing). No persistence —
        this is "what's configured right now," not history; `_job_log`
        (relay's own table) is where history lives."""
        out: list[dict] = []
        for queue, broker in self._brokers.items():
            for name, task in broker.get_all_tasks().items():
                for sched in task.labels.get("schedule", []):
                    if "cron" in sched:
                        out.append({"task_name": name, "queue": queue, "cron": sched["cron"]})
        return out

    # ------------------------------------------------------------------ #
    # Task registration — internal power-source surface, not the intended
    # dev-facing API. Business plugins should call arc.relay.task(...)/
    # arc.relay.register_tasks(...) instead (docs/arc.MD §3.15) — relay is
    # the facade every plugin writes against, the same posture already
    # established for cache_get/cache_set/lock (redix): relay delegates
    # here automatically when lineup is installed, and a plugin never
    # needs to know or declare a dependency on lineup itself to use it.
    # register_tasks()/task() below stay public (relay's own
    # implementation calls straight through to them, and direct use is
    # never actually wrong) — just not what a plugins/<plugin>/tasks/*.py
    # file should reach for first.
    #
    # Same directory-loading pattern relay.register_hooks()/register_api()
    # use (import each file under a deterministic synthetic module name,
    # tracking which plugin is "currently loading" so a decorator used
    # inside the file can attribute itself correctly).
    # ------------------------------------------------------------------ #
    def register_tasks(self, tasks_dir: str | Path) -> None:
        tasks_dir = Path(tasks_dir)
        if not tasks_dir.exists():
            return
        plugin = self._kernel.current_plugin() or "<direct>"
        for path in sorted(tasks_dir.glob("*.py")):
            self._loading_plugin = plugin
            try:
                module_name = f"_arc_lineup_tasks_{plugin}_{path.stem}"
                spec = importlib.util.spec_from_file_location(module_name, path)
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
            finally:
                self._loading_plugin = None

    def task(
        self, *, queue: str = DEFAULT_QUEUE, cron: str | None = None
    ) -> Callable[[Callable[..., Awaitable[Any]]], Any]:
        """Prefer `arc.relay.task(...)` in plugin code (docs/arc.MD §3.15)
        — this is what it delegates to when lineup is installed, exposed
        directly here mainly for lineup's own CLI/tests.

        `@arc.lineup.task(queue="default")` — a durable, on-demand job,
        dispatched via `arc.lineup.enqueue(fn, ...)` (or, via relay,
        `arc.relay.enqueue(fn, ...)`).

        `@arc.lineup.task(queue="default", cron="0 23 * * *")` — the same,
        plus a cron schedule (`arc lineup scheduler` fires it automatically;
        it's still independently callable via `enqueue()` too, e.g. "run
        the nightly job right now" from an admin action). The cron string
        is validated immediately, not left to fail silently the first time
        the scheduler polls it."""

        def decorator(fn: Callable[..., Awaitable[Any]]) -> Any:
            plugin = self._loading_plugin or self._kernel.current_plugin() or "<direct>"
            task_name = f"{plugin}.{fn.__name__}"
            if task_name in self._tasks:
                raise RuntimeError(f"lineup task '{task_name}' is already registered.")

            # Captured off the ORIGINAL fn, before `wrapped` below replaces
            # it — see _task_paths' own docstring in __init__.
            fn_qualname = getattr(fn, "__qualname__", "")
            if fn_qualname and "<locals>" not in fn_qualname:
                self._task_paths[task_name] = (fn.__module__, fn_qualname)

            labels: dict[str, Any] = {}
            if cron is not None:
                try:
                    pycron.is_now(cron, datetime.now(tz=timezone.utc))
                except ValueError as exc:
                    raise CronValueError(
                        f"lineup task '{task_name}': invalid cron expression {cron!r} — {exc}"
                    ) from exc
                # Registering this label is the ENTIRE effect of `cron=`.
                # It does not call fn, schedule an immediate run, or touch
                # the broker's queue in any way — it only becomes visible
                # to arc.lineup.run_scheduler()'s LabelScheduleSource,
                # which fires it at its real next occurrence, never before.
                labels["schedule"] = [{"cron": cron, "cron_offset": _local_cron_offset()}]

            # `context: Context = TaskiqDepends()` is TaskIQ's own DI
            # mechanism (the same one FastAPI's Depends() is modeled on) —
            # deliberately NOT wrapped with functools.wraps(fn), since that
            # would copy fn's own __annotations__ onto this wrapper and
            # silently erase the `context` parameter TaskIQ needs to see to
            # inject it at all. `context.message.labels` is the ONLY place
            # that reveals whether THIS SPECIFIC invocation came from `arc
            # lineup scheduler` (which stamps a `schedule_id` label on
            # every kick, verified directly: a plain .kiq() call carries no
            # such label, an AsyncKicker(...).with_labels(schedule_id=...)
            # kick — exactly what TaskiqScheduler.on_ready does — does) or
            # from a normal enqueue() — the task's own STATIC `schedule`
            # label only says "this task CAN be scheduled," not "this run
            # WAS."
            async def wrapped(*args: Any, context: Context = TaskiqDepends(), **kwargs: Any) -> Any:
                started_at = datetime.now(timezone.utc)
                labels = context.message.labels
                job_type = "Scheduler" if labels.get("schedule_id") else "Task"
                request_id, triggered_by = self._context_from(labels)
                # A Scheduler-fired run never went through enqueue() (cron
                # dispatch is TaskIQ's own internal scheduler loop, not
                # something this module wraps at all) — so there is no
                # Queued row for this task_id to update yet. _log_job_running
                # is a harmless no-op in that case; _log_job_finished's own
                # fallback insert (below) is what actually creates the row,
                # exactly as this file always did before Queued tracking
                # existed.
                await self._log_job_running(task_id=context.message.task_id, started_at=started_at)
                status, error = "success", None
                try:
                    # The job runs with the same call context the process
                    # that queued it had — so arc.relay.context() inside a
                    # durable job answers "which request, which user" just
                    # like it does in the process that enqueued it.
                    with self._bind_context(labels):
                        return await fn(*args, **kwargs)
                except asyncio.CancelledError:
                    # See the identical branch in _register_dispatch_task's
                    # own _dispatch above for the full reasoning — this is
                    # the SAME bug, in the OTHER dispatch wrapper (the one
                    # @arc.relay.task(...)/@arc.lineup.task(...)-declared
                    # jobs actually run through, arguably the more common
                    # path of the two). CancelledError is a BaseException;
                    # `except Exception` below never sees it, so `status`
                    # would otherwise stay "success" for a job a worker
                    # shutdown or timeout cut off mid-flight.
                    status, error = "cancelled", "job was cancelled (worker shutdown or timeout)"
                    raise
                except Exception as exc:
                    status, error = "failed", f"{type(exc).__name__}: {exc}"
                    raise
                finally:
                    await self._log_job_finished(
                        task_id=context.message.task_id,
                        task_name=task_name,
                        queue=queue,
                        job_type=job_type,
                        queued_by=plugin,
                        status=status,
                        error=error,
                        started_at=started_at,
                        finished_at=datetime.now(timezone.utc),
                        request_id=request_id,
                        triggered_by=triggered_by,
                    )

            broker = self._broker_for(queue)
            decorated = broker.task(task_name=task_name, **labels)(wrapped)
            self._tasks[task_name] = decorated
            self._task_queue[task_name] = queue
            return decorated

        return decorator

    # ------------------------------------------------------------------ #
    # _job_log lifecycle — Queued -> Running -> success/failed. `_job_log`
    # is owned by `relay` (docs/arc.MD §3.11/§3.15) — lineup just writes
    # into it, the same way any plugin can touch a table it doesn't own
    # without needing to own its schema (ownership only matters for
    # migration/diffing, §3.9). Every write here is best-effort: a DB
    # hiccup logging a job must never mask the real job's own outcome.
    #
    # One row per job, updated in place across its lifetime — matched by
    # `task_id` (TaskIQ's own id, stamped on the message at kick time,
    # readable both from what .kiq() returns and from context.message on
    # the worker side). _log_job_queued (called from enqueue()/
    # enqueue_by_path(), the producer side — BEFORE any worker has touched
    # the job) inserts the row; _log_job_running and _log_job_finished
    # (both called from the dispatch wrappers below, the CONSUMER side)
    # update that same row by task_id rather than inserting a fresh one,
    # so the Execution Log shows one job's whole life as one row
    # transitioning states, not three disconnected entries.
    #
    # Deliberately NOT covering every dispatch path: a Scheduler-fired
    # cron job never goes through enqueue()/enqueue_by_path() at all (it
    # fires via TaskIQ's own internal scheduler loop), so it never gets a
    # Queued row — _log_job_finished's fallback INSERT below is exactly
    # today's original single-write-at-the-end behavior, preserved
    # unchanged for that case. The in-process fallback (no lineup
    # installed) is untouched too — relay's own enqueue() fallback writes
    # `_job_log` itself, independently of this file, and a job there runs
    # essentially instantly (asyncio.create_task, no real queue), so
    # there's no meaningful "queued and waiting" period to show anyway.
    # ------------------------------------------------------------------ #
    async def _log_job_queued(
        self,
        *,
        task_id: str,
        task_name: str,
        queue: str,
        job_type: str,
        queued_by: str | None,
        request_id: str | None,
        triggered_by: str | None,
        payload: dict | None = None,
    ) -> None:
        """`payload`, when present, is what makes this row durable — see
        enqueue()/enqueue_by_path() below, which write it BEFORE ever
        touching Redis/Valkey, and _reap_and_run_stale, which reconstructs and
        runs a job from `payload` alone if nothing ever picks it up off
        Redis/Valkey. None (a task whose original fn wasn't cleanly resolvable,
        docs on _task_paths) means this row is observability-only, exactly
        as every _job_log row was before durability existed — the insert
        itself already best-effort (a DB hiccup here must not be treated
        as "the job never got queued," only logged): if `payload` happens
        to contain something that can't survive a JSONB round trip, this
        whole insert fails the same way any other DB hiccup would, and the
        job still dispatches normally over Redis/Valkey right after — no
        durability for that one call, no other change in behavior either."""
        try:
            await arc.psqldb.insert(
                "_job_log",
                {
                    "task_id": task_id,
                    "task_name": task_name,
                    "queue": queue,
                    "executor": "lineup",
                    "job_type": job_type,
                    "queued_by": queued_by,
                    "status": "Queued",
                    "request_id": request_id,
                    "triggered_by": triggered_by,
                    "payload": payload,
                    "queued_at": datetime.now(timezone.utc),
                },
            )
        except Exception as exc:
            logger.error(f"failed to write Queued _job_log row for {task_name}: {exc}")

    async def _log_job_running(self, *, task_id: str, started_at: datetime) -> None:
        try:
            async with arc.psqldb.acquire() as conn:
                await conn.execute(
                    'UPDATE "_job_log" SET status = $1, started_at = $2 WHERE task_id = $3',
                    "Running",
                    started_at,
                    task_id,
                )
        except Exception as exc:
            logger.error(f"failed to mark _job_log row Running for task_id {task_id}: {exc}")

    async def _log_job_finished(
        self,
        *,
        task_id: str,
        task_name: str,
        queue: str,
        job_type: str,
        queued_by: str | None,
        status: str,
        error: str | None,
        started_at: datetime,
        finished_at: datetime,
        request_id: str | None = None,
        triggered_by: str | None = None,
    ) -> None:
        duration_ms = int((finished_at - started_at).total_seconds() * 1000)
        try:
            async with arc.psqldb.acquire() as conn:
                result = await conn.execute(
                    'UPDATE "_job_log" SET status = $1, error = $2, started_at = $3, '
                    'finished_at = $4, duration_ms = $5 WHERE task_id = $6',
                    status,
                    error,
                    started_at,
                    finished_at,
                    duration_ms,
                    task_id,
                )
            if result.endswith(" 0"):
                # No Queued row to update (a Scheduler-fired job, per this
                # method's own docstring above) — fall back to a plain
                # insert, exactly this file's original, single-write
                # behavior.
                await arc.psqldb.insert(
                    "_job_log",
                    {
                        "task_id": task_id,
                        "task_name": task_name,
                        "queue": queue,
                        "executor": "lineup",
                        "job_type": job_type,
                        "queued_by": queued_by,
                        "status": status,
                        "error": error,
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "duration_ms": duration_ms,
                        # Which request queued this job, and as whom — read
                        # off the message labels the enqueuing PROCESS
                        # stamped, so the trail survives that process being
                        # long gone.
                        "request_id": request_id,
                        "triggered_by": triggered_by,
                    },
                )
        except Exception as exc:
            logger.error(f"failed to write _job_log row for {task_name}: {exc}")

    # ------------------------------------------------------------------ #
    # Call-context propagation across the process boundary (docs/arc.MD
    # §3.11's CallContext). A durable job runs in a DIFFERENT process than
    # the one that queued it, so relay's contextvar cannot reach it — the
    # context travels as message labels instead: stamped on the kick here,
    # rebound around the job on the worker side.
    #
    # lineup deliberately knows NOTHING about what is in those labels. It
    # asks relay to encode them, moves an opaque dict of strings, and asks
    # relay to decode them again. That keeps the dependency direction
    # correct — relay optionally depends on lineup, never the reverse, so
    # importing relay from here would be backwards — and means a future
    # field on CallContext needs no edit in this file at all.
    #
    # The kernel lookup is lazy, PER CALL, for the same reason gateway's
    # identity_middleware looks up authn per request rather than once at
    # construction: arc's resolver only orders HARD requires strictly
    # (§3.1), and lineup does not require relay at all, so a boot-time
    # capture could easily miss it.
    # ------------------------------------------------------------------ #
    def _relay(self) -> Any | None:
        return self._kernel.get("relay") if self._kernel.has("relay") else None

    def _context_labels(self) -> dict[str, str]:
        """The ambient call context, encoded for the wire — `{}` when relay
        isn't installed, is too old to know about this, or there simply is
        no context (a CLI run, a scheduled job with no originating
        request). Never raises: failing to attach provenance metadata must
        not stop a job from being queued."""
        relay = self._relay()
        encode = getattr(relay, "context_labels", None)
        if not callable(encode):
            return {}
        try:
            return encode() or {}
        except Exception as exc:  # noqa: BLE001 - provenance must never block dispatch
            logger.warning(f"could not encode call context for dispatch: {exc}")
            return {}

    def _bind_context(self, labels: Any):
        """Rebind the call context a job was queued with, for the duration
        of that job — the worker-side counterpart of _context_labels().
        Falls back to a no-op context manager when relay isn't installed or
        the labels can't be read, so a job always runs either way."""
        relay = self._relay()
        decode = getattr(relay, "use_context_labels", None)
        if not callable(decode):
            return contextlib.nullcontext()
        try:
            return decode(labels)
        except Exception as exc:  # noqa: BLE001 - provenance must never break the job
            logger.warning(f"could not restore call context for job: {exc}")
            return contextlib.nullcontext()

    @staticmethod
    def _context_from(labels: Any) -> tuple[str | None, str | None]:
        """(request_id, triggered_by) straight off the message labels, for
        the `_job_log` row — read directly rather than through relay's
        contextvar so the log line is still correct when relay isn't
        installed at all."""
        labels = labels or {}
        return labels.get("arc_ctx_request_id"), labels.get("arc_ctx_user")

    def tasks(self) -> dict[str, Any]:
        """Every registered task, keyed by its `{plugin}.{fn.__name__}`
        name — the introspection counterpart to relay.whitelisted(). The
        only correct way to get a real task object outside the plugin that
        defined it: a plain `from some_plugin.tasks.x import y` re-imports
        the file under Python's normal module-cache key instead of the
        synthetic one register_tasks() used, silently creating a SECOND,
        disconnected task registration under task_name "<direct>.y" (no
        self._loading_plugin set, since that import didn't go through
        register_tasks() at all) — found exactly this way while verifying
        this plugin against a real boot."""
        return dict(self._tasks)

    def is_task(self, fn: Any) -> bool:
        """Whether `fn` is something `.enqueue()` can actually dispatch —
        i.e. the object `@arc.lineup.task(...)` returned, not the original
        plain function. Used by relay's enqueue() upgrade path (docs/
        arc.MD §3.11/§3.14) to decide whether a call can go durable."""
        return fn in self._tasks.values()

    async def enqueue(self, fn: Any, *args: Any, **kwargs: Any) -> None:
        """`fn` must already be `@arc.lineup.task(...)`-decorated — a plain
        function can't be dispatched to a worker in a different process at
        all (there'd be nothing for that process to import and run), so
        this raises immediately rather than silently doing something
        weaker."""
        if not self.is_task(fn):
            name = getattr(fn, "__name__", repr(fn))
            raise TypeError(
                f"'{name}' is not a registered lineup task — decorate it with "
                f"@arc.lineup.task(...) in a plugins/<plugin>/tasks/*.py file "
                f"loaded via register_tasks() first, or use enqueue_by_path()/"
                f"arc.relay.enqueue() for a plain function instead."
            )
        labels = self._context_labels()
        request_id, triggered_by = self._context_from(labels)
        queue = self._task_queue.get(fn.task_name, DEFAULT_QUEUE)

        # Durability fix (docs/"Missing Failure-Mode Audits", items 15/19):
        # the Queued row — WITH a reconstructable payload, when this task's
        # original fn resolved cleanly at registration (_task_paths) — is
        # written FIRST, before Redis/Valkey is touched at all, using a
        # task_id WE generate rather than one TaskIQ hands back afterward
        # (AsyncKicker.with_task_id lets a kick carry a caller-chosen id).
        # That ordering is the whole point: if the write never happens (a
        # write that then rolls back — see relay/background_jobs.py's own
        # gate on this, which is what decides whether enqueue() is even
        # called at all), no message is EVER dispatched. If the write
        # happens but the kick then fails (the Redis/Valkey server is
        # down), the job is NOT lost the way it used to be — it's a
        # durable "Queued" row with a payload, and _reap_and_run_stale
        # will find and run it directly off that row on its next pass,
        # without needing Redis/Valkey at all.
        task_id = str(uuid.uuid4())
        original = self._task_paths.get(fn.task_name)
        payload = (
            {"module": original[0], "qualname": original[1], "args": list(args), "kwargs": kwargs}
            if original is not None
            else None
        )
        await self._log_job_queued(
            task_id=task_id,
            task_name=fn.task_name,
            queue=queue,
            job_type="Task",
            queued_by=fn.task_name.split(".")[0],
            request_id=request_id,
            triggered_by=triggered_by,
            payload=payload,
        )

        kicker = fn.kicker().with_task_id(task_id)
        try:
            # .with_labels(...) rather than a plain .kiq(): this is
            # TaskIQ's own supported way to attach PER-KICK labels (the same
            # mechanism its scheduler uses to stamp schedule_id), as opposed
            # to the STATIC labels declared once at broker.task() time.
            if labels:
                await kicker.with_labels(**labels).kiq(*args, **kwargs)
            else:
                await kicker.kiq(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - the row is already durable; see docstring above
            logger.warning(
                f"lineup kick failed for {fn.task_name} (task_id={task_id}): {exc} — "
                f"job remains durably queued, the reaper will pick it up"
            )

    # ------------------------------------------------------------------ #
    # Ad hoc dispatch — no @task decoration, no tasks/ directory. Only the
    # function's own (module, qualname) crosses the wire, never the function
    # itself (TaskIQ sends a task NAME + arguments, always) — a worker
    # re-imports the real object fresh when the job runs, on its own side.
    # ------------------------------------------------------------------ #
    def check_resolvable(self, fn: Any) -> tuple[str, str]:
        """Validates `fn` can actually be found again by a DIFFERENT
        process later — raises TypeError immediately if not, rather than
        letting a bad reference surface only once a worker tries and fails
        to run it (the same "fail fast, before it gets weird" posture used
        everywhere else in this project, e.g. psqldb's schema validation).

        Rejects: a lambda (no stable name); a closure/nested function
        (`__qualname__` contains "<locals>" — there's no path to it from
        module scope at all); and anything that doesn't resolve back to
        the EXACT SAME object at its own declared module + qualname (a
        name that was reassigned after definition, or a decorator that
        wrapped it without `functools.wraps`)."""
        name = getattr(fn, "__name__", None)
        qualname = getattr(fn, "__qualname__", None)
        module_path = getattr(fn, "__module__", None)

        if name == "<lambda>":
            raise TypeError(
                "can't enqueue a lambda — it has no stable, importable name a worker "
                "process could resolve later. Give it a real module-level def instead."
            )
        if not qualname or not module_path:
            raise TypeError(
                f"{fn!r} has no __module__/__qualname__ — not a plain importable function."
            )
        if "<locals>" in qualname:
            raise TypeError(
                f"'{qualname}' is defined inside another function (a closure) — it has no path "
                f"a worker process can import later. Move it to module level."
            )

        module = sys.modules.get(module_path)
        if module is None:
            try:
                module = importlib.import_module(module_path)
            except ImportError as exc:
                raise TypeError(f"'{module_path}' is not importable — {exc}") from exc

        resolved: Any = module
        try:
            for part in qualname.split("."):
                resolved = getattr(resolved, part)
        except AttributeError:
            raise TypeError(
                f"'{module_path}.{qualname}' does not resolve to a real attribute — "
                f"can't be enqueued as a plain background job."
            ) from None
        if resolved is not fn:
            raise TypeError(
                f"'{module_path}.{qualname}' resolves to a DIFFERENT object than the one "
                f"passed in — it may have been reassigned after definition, or wrapped by a "
                f"decorator that doesn't preserve identity. Can't enqueue it by path."
            )
        return module_path, qualname

    def _dispatch_module_allowed(self, module_path: str) -> bool:
        """The generic dispatch task imports-and-calls whatever
        (module, qualname) arrives in a Redis/Valkey message — without a check,
        anyone with write access to that instance gets arbitrary code execution in
        the worker (e.g. `("os", "system", ["..."], {})`). This bounds it
        to code that belongs to this project: a module whose root is an
        installed plugin's own package (flat layout: package name == plugin
        name, §3.7), or one of the synthetic module names relay/lineup's
        own directory loaders register (api/hooks/tasks files). Checked on
        the WORKER side, where it matters — check_resolvable() on the
        enqueue side is a convenience check, not a security boundary."""
        root = module_path.split(".")[0]
        if root.startswith("_arc_relay_") or root.startswith("_arc_lineup_"):
            return True
        caps = self._kernel.capabilities()
        plugin_names = {cap.plugin for cap in caps.values()}
        return root in plugin_names or root in caps

    def _register_dispatch_task(self, broker: ListQueueBroker, queue: str) -> None:
        """The generic task that actually does the dynamic import + call,
        registered unconditionally the moment `_broker_for` creates a
        broker for `queue` — see the long comment on `_broker_for` above
        for why this can't be deferred until an actual enqueue_by_path()
        call (a worker process needs it registered too, and never calls
        enqueue_by_path() itself).

        `job_type` is always "Task" here, never "Scheduler", by
        construction — this task never declares a `schedule` label of its
        own (unlike `task()`'s wrapper above), so `arc lineup scheduler`'s
        `LabelScheduleSource` can never pick it up in the first place; no
        need to inspect a Context to tell the two apart the way `task()`'s
        wrapper does."""

        # `context: Context = TaskiqDepends()` is TaskIQ's own DI mechanism,
        # the same one task()'s wrapper above uses — it's the only way to
        # see this specific invocation's message labels, which is where the
        # call context of the process that queued the job travels.
        async def _dispatch(
            module_path: str,
            qualname: str,
            args: list,
            kwargs: dict,
            context: Context = TaskiqDepends(),
        ) -> None:
            started_at = datetime.now(timezone.utc)
            task_name = f"{module_path}.{qualname}"
            queued_by = module_path.split(".")[0] if module_path else None
            labels = context.message.labels
            request_id, triggered_by = self._context_from(labels)
            await self._log_job_running(task_id=context.message.task_id, started_at=started_at)
            status, error = "success", None
            try:
                if not self._dispatch_module_allowed(module_path):
                    raise PermissionError(
                        f"refusing to dispatch '{module_path}.{qualname}' — its root module is not "
                        f"an installed ARC plugin package (or a relay/lineup-loaded module). "
                        f"lineup only executes code belonging to this project's own plugins."
                    )
                module = importlib.import_module(module_path)
                target: Any = module
                for part in qualname.split("."):
                    target = getattr(target, part)
                with self._bind_context(labels):
                    await target(*args, **kwargs)
            except asyncio.CancelledError:
                # A worker shutting down (SIGTERM, a deploy, `arc lineup
                # worker` stopping) cancels every in-flight dispatch task —
                # CancelledError is a BaseException, not an Exception, so
                # it would otherwise fall straight through the `except
                # Exception` below and leave `status` at its "success"
                # default: the job never ran to completion, but _job_log
                # would say it did, with nothing to ever suggest otherwise.
                # Recorded here, distinctly from an ordinary failure (this
                # job may have done partial, unknown work — not the same
                # claim as "it ran and raised"), and always re-raised:
                # cancellation must never be swallowed, the caller's own
                # shutdown/timeout logic depends on it actually propagating.
                status, error = "cancelled", "job was cancelled (worker shutdown or timeout)"
                raise
            except Exception as exc:
                status, error = "failed", f"{type(exc).__name__}: {exc}"
                raise
            finally:
                await self._log_job_finished(
                    task_id=context.message.task_id,
                    task_name=task_name,
                    queue=queue,
                    job_type="Task",
                    queued_by=queued_by,
                    status=status,
                    error=error,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    request_id=request_id,
                    triggered_by=triggered_by,
                )

        self._dispatch_tasks[queue] = broker.task(task_name=f"lineup._dispatch.{queue}")(_dispatch)

    async def enqueue_by_path(
        self, fn: Any, *args: Any, queue: str = DEFAULT_QUEUE, **kwargs: Any
    ) -> None:
        """Enqueue a PLAIN function — no `@arc.lineup.task(...)`/
        `@arc.relay.task(...)` decoration needed at all, callable from
        anywhere. Prefer `arc.relay.enqueue(fn, queue=..., ...)` in plugin
        code (docs/arc.MD §3.15); this exists on lineup directly mainly
        for relay's own delegation.

        Validates `fn` via check_resolvable() before ever touching Redis/Valkey
        — a bad reference fails here, synchronously, not inside a worker
        process minutes or hours later."""
        module_path, qualname = self.check_resolvable(fn)
        self._broker_for(queue)  # ensures the dispatch task below actually exists
        await self._ensure_started(
            queue
        )  # a queue first touched after open() still gets a real startup()
        dispatch = self._dispatch_tasks[queue]
        labels = self._context_labels()
        request_id, triggered_by = self._context_from(labels)

        # Same write-before-push ordering as enqueue() above, and the same
        # reasoning — see that method's docstring. check_resolvable()
        # already proved module_path/qualname resolve, so this payload is
        # always reconstructable (unlike enqueue()'s, which can be None).
        task_id = str(uuid.uuid4())
        payload = {"module": module_path, "qualname": qualname, "args": list(args), "kwargs": kwargs}
        await self._log_job_queued(
            task_id=task_id,
            task_name=f"{module_path}.{qualname}",
            queue=queue,
            job_type="Task",
            queued_by=module_path.split(".")[0] if module_path else None,
            request_id=request_id,
            triggered_by=triggered_by,
            payload=payload,
        )

        kicker = dispatch.kicker().with_task_id(task_id)
        try:
            if labels:
                await kicker.with_labels(**labels).kiq(module_path, qualname, list(args), kwargs)
            else:
                await kicker.kiq(module_path, qualname, list(args), kwargs)
        except Exception as exc:  # noqa: BLE001 - the row is already durable; see enqueue()'s docstring
            logger.warning(
                f"lineup kick failed for {module_path}.{qualname} (task_id={task_id}): {exc} — "
                f"job remains durably queued, the reaper will pick it up"
            )

    # ------------------------------------------------------------------ #
    # Lifecycle — async def open()/close(), the same duck-typed contract
    # every other capability with real connections uses (psqldb/redix);
    # Gateway's ASGI lifespan calls both automatically for every capability
    # that has them (gateway/__init__.py's _open_all_capabilities). A CLI
    # process (worker/scheduler/status) isn't behind Gateway's lifespan at
    # all, so it calls these explicitly itself, same as authn's admin CLI
    # already does for psqldb/redix.
    # ------------------------------------------------------------------ #
    async def open(self) -> None:
        self._opened = True
        for name, broker in self._brokers.items():
            if name not in self._started:
                await broker.startup()
                self._started.add(name)
        self.start_reaper()

    async def _ensure_started(self, queue: str) -> None:
        """Start a lazily-created broker if the provider is already open —
        see __init__'s lifecycle comment."""
        if self._opened and queue not in self._started:
            await self._brokers[queue].startup()
            self._started.add(queue)

    async def close(self) -> None:
        await self.stop_reaper()
        for name, broker in self._brokers.items():
            if name in self._started:
                await broker.shutdown()
        self._started.clear()
        self._opened = False

    async def health(self) -> dict:
        return {"ok": True, "queues": self.queues()}

    # ------------------------------------------------------------------ #
    # Durable-queue recovery — the worker-side half of items 15/19's fix
    # (see enqueue()/enqueue_by_path()'s docstrings for the write side).
    # start_reaper()/stop_reaper() are separate from open()/close() (which
    # call them) because `arc lineup worker` deliberately bypasses open()
    # for its own brokers (it sets is_worker_process on each one first,
    # cli.py's worker() command) while still needing the reaper running —
    # calling start_reaper() directly there gets it without double-starting
    # any broker.
    # ------------------------------------------------------------------ #
    def start_reaper(self) -> None:
        if self._reap_task is None:
            self._reap_task = asyncio.create_task(self._reap_loop())

    async def stop_reaper(self) -> None:
        if self._reap_task is not None:
            self._reap_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reap_task
            self._reap_task = None

    async def _reap_loop(self) -> None:
        while True:
            await asyncio.sleep(_REAP_INTERVAL_SECONDS)
            try:
                await self._reap_and_run_stale()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - one bad pass must not kill the loop
                logger.error(f"lineup durable job reaper pass failed: {exc}")

    async def _reap_and_run_stale(self, limit: int = 10) -> int:
        """Claims lineup-owned jobs (`executor='lineup'`) abandoned by a
        dead process — stuck 'Queued' because the kick to Redis/Valkey never
        landed or nobody's listening on that queue right now, or stuck
        'Running' because the worker process that popped it off Redis/Valkey died
        mid-job (ListQueueBroker's own known limitation — see this module's
        docstring: a message is gone from the list the instant a worker BRPOPs
        it, before the job finishes, so a worker crash after that point has
        nothing left to redeliver from Redis/Valkey itself). `FOR UPDATE SKIP
        LOCKED` lets any number of processes with lineup open run this
        concurrently with zero coordination.

        Runs the job DIRECTLY off the row's own `payload` — dynamic import,
        same _dispatch_module_allowed() trust boundary as a Redis/Valkey-delivered
        message gets (a reaped row is reconstructed from data this process
        itself wrote via enqueue()/enqueue_by_path(), not something arriving
        over the wire from elsewhere, but there is no reason to hold it to a
        LOOSER standard than that path just because of that) — never
        through TaskIQ/Redis-or-Valkey at all, which is the whole point: this is the
        recovery path for when Redis/Valkey already failed to deliver or never got
        the chance to.

        Only touches `executor='lineup', payload IS NOT NULL` rows — a
        Scheduler-fired job (never went through enqueue() at all) or a
        pre-durability legacy row has no payload to run from and is left
        alone; relay runs the equivalent reaper for its own `executor=
        'relay'` rows (relay/background_jobs.py's _reap_stale_jobs)."""
        started_at = datetime.now(timezone.utc)
        lease_until = started_at + timedelta(seconds=_JOB_LEASE_SECONDS)
        async with arc.psqldb.acquire() as conn:
            claimed = await conn.fetch(
                'UPDATE "_job_log" SET status=$1, claimed_by=$2, lease_expires_at=$3, '
                "started_at=COALESCE(started_at, $4) "
                "WHERE id IN ("
                '  SELECT id FROM "_job_log" '
                "  WHERE executor=$5 AND payload IS NOT NULL "
                "    AND status IN ('Queued', 'Running') "
                "    AND (lease_expires_at IS NULL OR lease_expires_at < now()) "
                "  ORDER BY queued_at NULLS FIRST "
                "  LIMIT $6 "
                "  FOR UPDATE SKIP LOCKED"
                ") RETURNING *",
                "Running",
                self._worker_id,
                lease_until,
                started_at,
                "lineup",
                limit,
            )
        for row in claimed:
            try:
                await self._run_claimed_row(row)
            except Exception as exc:  # noqa: BLE001 - already recorded on the row; keep reaping
                logger.error(f"reaped lineup job {row['id']} failed: {exc}")
        return len(claimed)

    async def _run_claimed_row(self, row: Any) -> None:
        payload = row["payload"] or {}
        module_path, qualname = payload.get("module"), payload.get("qualname")
        args, kwargs = payload.get("args") or [], payload.get("kwargs") or {}
        row_id, task_name = row["id"], row["task_name"]
        started_at = row["started_at"] or datetime.now(timezone.utc)
        status, error = "success", None
        try:
            if not self._dispatch_module_allowed(module_path):
                raise PermissionError(
                    f"refusing to run reaped job '{module_path}.{qualname}' — its root module "
                    f"is not an installed ARC plugin package."
                )
            module = importlib.import_module(module_path)
            target: Any = module
            for part in qualname.split("."):
                target = getattr(target, part)
            await target(*args, **kwargs)
        except asyncio.CancelledError:
            status, error = "cancelled", "reaper task was cancelled (process shutdown)"
            raise
        except Exception as exc:
            status, error = "failed", f"{type(exc).__name__}: {exc}"
            raise
        finally:
            finished_at = datetime.now(timezone.utc)
            try:
                async with arc.psqldb.acquire() as conn:
                    await conn.execute(
                        'UPDATE "_job_log" SET status=$1, error=$2, finished_at=$3, '
                        "duration_ms=$4 WHERE id=$5",
                        status,
                        error,
                        finished_at,
                        int((finished_at - started_at).total_seconds() * 1000),
                        row_id,
                    )
            except Exception as log_exc:
                logger.error(f"failed to write finished _job_log row for reaped {task_name}: {log_exc}")


def register(kernel: Any) -> None:
    redix = kernel.get("redix")
    provider = LineupProvider(kernel, redis_url=redix.url)
    # psqldb: needed to write into relay's own `_job_log` table (§3.11/
    # §3.15) whenever a task actually runs — not for durable dispatch
    # itself, which only ever needs redix. Declared as a hard requirement
    # (not best-effort/optional) so a project missing it gets a clear
    # boot-time error instead of every task's log write silently failing
    # forever.
    kernel.export(CAPABILITY, provider, requires=["redix", "psqldb"], optional_requires=[])
