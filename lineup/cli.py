"""
lineup.cli — `arc lineup ...` commands.

Mounted via the `arc.plugins.cli` entry point, same mechanism as
psqldb.cli/redix.cli/authn.cli. `worker` and `scheduler` do a real
`arc.boot()` (task/schedule registration only happens inside every
plugin's own register(), same reason authn's admin CLI needs a real boot
— there's no other way to discover what's actually registered). `status`
stays a lightweight, no-boot connectivity probe, same shape as redix's own
status/connect (reads the URL straight off disk).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
import warnings
from urllib.parse import urlparse

import redis as redis_sync
import typer
from rich.console import Console
from taskiq.cli.scheduler.run import SchedulerLoop
from taskiq.receiver import Receiver
from taskiq.scheduler.scheduler import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource

import arc
from arc.runtime import find_project_root
from arc.settings import SettingsManager

# A narrow, precedented exception, same shape as authn.cli importing
# psqldb.validation.ValidationError directly (docs/arc.MD §3.11) — lineup
# hard-requires redix and deliberately never declares its own duplicate
# settings key (lineup/__init__.py's module docstring), so this constant
# IS the single source of truth for "which settings key holds the URL".
# Hardcoding the string "redix_url" here instead would silently go stale
# if redix ever renamed it; importing the constant can't.
from redix import URL_KEY

app = typer.Typer(help="Commands for the lineup provider (durable jobs + scheduling).")
console = Console()
err_console = Console(stderr=True, style="bold red")
logger = logging.getLogger("lineup.cli")


def _url_from_disk() -> str:
    """Same lightweight, no-boot lookup redix.cli._url() uses — reused here
    only for `status`, which deliberately doesn't need a full arc.boot()."""
    root = find_project_root()
    if root is None:
        err_console.print("Not inside an ARC project (no .arc/arc.toml found here or in any parent).")
        raise typer.Exit(code=1)
    mgr = SettingsManager(root / ".arc")
    url = mgr.get(URL_KEY, reveal=True)
    if url is None:
        err_console.print(f"'{URL_KEY}' is not set. Run: arc settings set {URL_KEY} redis://host:6379/0 --secret")
        raise typer.Exit(code=1)
    return url


def _live_kernel():
    """The real Kernel instance arc.boot() just built. arc's own __getattr__/
    __dir__ (arc/arc/__init__.py) read this exact same module-private state
    to resolve arc.<capability> — there's no public `arc.kernel` capability
    (the Kernel is the container, not something a plugin exports, docs/
    arc.MD §3.1), so this is the sanctioned way to reach it directly."""
    from arc import _state

    return _state.get_kernel()


async def _open_all_capabilities(*, exclude: frozenset[str] = frozenset()) -> None:
    """Same duck-typed open() sweep gateway.__init__._open_all_capabilities
    does for every ASGI-served request — a worker/scheduler process isn't
    behind Gateway's lifespan at all, so it has to do this itself. Needed
    because a task function is ordinary plugin code and may call
    arc.relay.*/arc.psqldb.*/arc.redix.* same as any request handler would.

    `exclude` lets worker/scheduler skip lineup's own brokers here — they
    set is_worker_process/is_scheduler_process on each broker themselves
    before calling broker.startup() directly, which this generic sweep
    can't know to do (TaskIQ's startup() fires a different event,
    WORKER_STARTUP vs CLIENT_STARTUP, depending on that flag — calling
    lineup.open() first would start every broker under the wrong one)."""
    for name, cap in _live_kernel().capabilities().items():
        if name in exclude:
            continue
        open_fn = getattr(cap.instance, "open", None)
        if callable(open_fn):
            await open_fn()


async def _close_all_capabilities(*, exclude: frozenset[str] = frozenset()) -> None:
    for name, cap in _live_kernel().capabilities().items():
        if name in exclude:
            continue
        close_fn = getattr(cap.instance, "close", None)
        if callable(close_fn):
            await close_fn()


def _boot() -> None:
    # worker/scheduler are long-running background processes — task code
    # (e.g. example_hr/tasks/onboarding.py's logger.info calls) needs a
    # configured root logger to actually be visible on the console, unlike
    # a short-lived request handler where nobody's watching stdout live.
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(name)s: %(message)s")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", arc.ArcAdvisory)
        arc.boot()


def _run_swallowing_shutdown_noise(coro: "asyncio.coroutines.Coroutine") -> None:
    """`asyncio.run(coro)`, but treats whatever surfaces AFTER a graceful
    stop was requested (Ctrl-C/SIGTERM) as expected noise rather than a
    crash. A BRPOP or the scheduler's poll loop cancelled mid-flight
    doesn't always unwind as a clean asyncio.CancelledError through
    taskiq_redis/redis-py/anyio (verified directly: a worker that
    successfully durably dispatched and ran a real job still printed a raw
    TimeoutError traceback from an in-flight BRPOP on shutdown) — real
    startup-time errors (Redis unreachable, an unknown --queues name) still
    surface normally, since _resolve_queues()/broker.startup() raise
    before the "listening on ..." banner ever prints, well before this
    swallows anything."""
    try:
        asyncio.run(coro)
    except typer.Exit:
        raise
    except (KeyboardInterrupt, SystemExit):
        pass
    except BaseException as exc:
        logger.debug(f"shutdown-time noise (harmless): {exc!r}")


def _resolve_queues(requested: str | None) -> list[str]:
    """No `--queues`: every queue something has already declared (a
    `@arc.relay.task(queue=...)` somewhere). An explicit `--queues=...`
    isn't validated against that list — it can name a queue nothing has
    pre-declared at all (a purely ad hoc one, only ever used via
    `arc.relay.enqueue(fn, queue="...")` calls), so each requested name is
    passed through `ensure_queue()`, which creates that queue's broker (+
    its dispatch task) right here if this process hasn't touched it yet.
    Without this, a worker told to listen on a brand-new ad hoc queue name
    would have nothing registered for it at all, since boot only ever
    registers PRE-declared tasks — found exactly this way, verifying an ad
    hoc job against a real worker process."""
    if not requested:
        return arc.lineup.queues()
    wanted = [q.strip() for q in requested.split(",") if q.strip()]
    for q in wanted:
        arc.lineup.ensure_queue(q)
    return wanted


@app.command()
def status() -> None:
    """Check connectivity to the Redis instance lineup would use (the same
    one redix is configured with — lineup has no separate URL of its own)."""
    url = _url_from_disk()
    parsed = urlparse(url)
    client = redis_sync.from_url(url, socket_connect_timeout=5, socket_timeout=5)
    try:
        start = time.monotonic()
        client.ping()
        elapsed = time.monotonic() - start
    except Exception as exc:
        err_console.print(f"lineup: FAILED to connect to {parsed.hostname}:{parsed.port or 6379} — {exc}")
        raise typer.Exit(code=1)
    finally:
        client.close()
    console.print(f"[bold green]lineup: OK[/bold green] ({elapsed * 1000:.0f}ms) via redix's redis_url")
    console.print("  run `arc lineup worker` / `arc lineup scheduler` to see registered queues.")


@app.command()
def worker(
    queues: str | None = typer.Option(
        None, "--queues", help="Comma-separated queue names to consume. Default: every registered queue."
    ),
) -> None:
    """Consume durable jobs from one or more named queues in this one
    process (a queue is a Redis LIST — one Receiver per queue, run
    concurrently via asyncio.gather, not one process per queue)."""
    _boot()

    async def _main() -> None:
        target = _resolve_queues(queues)
        if not target:
            err_console.print("no lineup queues are registered — nothing to consume.")
            raise typer.Exit(code=1)
        console.print(f"[bold]lineup worker[/bold] listening on: {', '.join(target)}")

        brokers = arc.lineup.broker_map()
        for name in target:
            brokers[name].is_worker_process = True
            await brokers[name].startup()
        await _open_all_capabilities(exclude=frozenset({"lineup"}))

        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, shutdown_event.set)

        receivers = [Receiver(broker=brokers[name]) for name in target]

        try:
            await asyncio.gather(*(r.listen(shutdown_event) for r in receivers))
        finally:
            console.print("[dim]lineup worker shutting down...[/dim]")
            for name in target:
                await brokers[name].shutdown()
            await _close_all_capabilities(exclude=frozenset({"lineup"}))

    _run_swallowing_shutdown_noise(_main())


@app.command()
def scheduler(
    queues: str | None = typer.Option(
        None, "--queues", help="Comma-separated queue names to poll for scheduled jobs. Default: every registered queue."
    ),
) -> None:
    """Poll every registered queue's cron-labeled tasks and dispatch each
    one at its real next occurrence — never at process startup, never at
    registration time (see lineup/__init__.py's module docstring for why
    this is guaranteed, not just intended: TaskIQ's own is_cron_task_now
    checks the real wall-clock minute against the cron expression on every
    poll, it never treats "just discovered this schedule" as "due")."""
    _boot()

    async def _main() -> None:
        target = _resolve_queues(queues)
        if not target:
            err_console.print("no lineup queues are registered — nothing to schedule.")
            raise typer.Exit(code=1)
        console.print(f"[bold]lineup scheduler[/bold] polling: {', '.join(target)}")

        brokers = arc.lineup.broker_map()
        loops = []
        for name in target:
            broker = brokers[name]
            broker.is_scheduler_process = True
            sched = TaskiqScheduler(broker=broker, sources=[LabelScheduleSource(broker)])
            for source in sched.sources:
                await source.startup()
            await sched.startup()
            loops.append(SchedulerLoop(sched))
        await _open_all_capabilities(exclude=frozenset({"lineup"}))

        run_task = asyncio.ensure_future(asyncio.gather(*(loop.run() for loop in loops)))
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, run_task.cancel)

        try:
            await run_task
        except asyncio.CancelledError:
            pass
        finally:
            console.print("[dim]lineup scheduler shutting down...[/dim]")
            for name in target:
                await brokers[name].shutdown()
            await _close_all_capabilities(exclude=frozenset({"lineup"}))

    _run_swallowing_shutdown_noise(_main())
