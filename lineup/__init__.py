"""
lineup — ARC provider plugin: durable background jobs + cron-style
scheduling, built on TaskIQ with a Redis broker (docs/arc.MD §7 Phase 6.5
follow-up — replaces `arc.relay.enqueue()`'s in-process-only fallback with
a real durable backend, and adds a scheduler that didn't exist at all
before this).

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
duplicate `lineup_redis_url` setting. Two independent Redis client
libraries end up talking to the same instance (redix's own `redis.asyncio`
client for cache/lock/pubsub, TaskIQ's own connection pool for the queue
lists) — that's fine, they don't share connections and don't need to; the
one thing that has to match is the URL itself.

Multiple named queues, not one global queue: `queue_name` on TaskIQ's own
`ListQueueBroker` maps directly to a distinct Redis LIST key, so "a
different type of queue" is just a different string — `@arc.lineup.task
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
    `(module, qualname)` over Redis (TaskIQ never sends code, only a name
    + arguments), and a worker re-imports the real function fresh when
    the job actually runs. The one real requirement this imposes — `fn`
    has to be a genuine plain, module-level function, not a lambda or a
    closure — is checked immediately, synchronously, at the call site
    (`check_resolvable()`), not discovered later inside a worker.

Known, deliberate limitation of the broker choice (ListQueueBroker, a
plain Redis LIST via BRPOP): a message is removed from the list the
instant a worker pops it, before the task finishes running — so a durable
job now survives the *enqueuing* process (Gateway) crashing or restarting
before a worker ever picks it up (the original problem this plugin
exists to fix), but does NOT survive a *worker* crashing mid-task after
having already popped the job. taskiq-redis also ships RedisStreamBroker
(consumer groups, ack/redelivery) for that stronger guarantee — not used
here, to keep the first version simple; swapping the broker class is a
contained, later change if a real need for it shows up (same
"don't build ahead of a real need" posture as everywhere else, docs/
arc.MD §7).
"""

from __future__ import annotations

import importlib.util
import logging
import sys
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
        self._dispatch_tasks: dict[str, Any] = {}
        self._loading_plugin: str | None = None
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
                job_type = "Scheduler" if context.message.labels.get("schedule_id") else "Task"
                status, error = "success", None
                try:
                    return await fn(*args, **kwargs)
                except Exception as exc:
                    status, error = "failed", f"{type(exc).__name__}: {exc}"
                    raise
                finally:
                    await self._write_job_log(
                        task_name=task_name,
                        queue=queue,
                        job_type=job_type,
                        queued_by=plugin,
                        status=status,
                        error=error,
                        started_at=started_at,
                        finished_at=datetime.now(timezone.utc),
                    )

            broker = self._broker_for(queue)
            decorated = broker.task(task_name=task_name, **labels)(wrapped)
            self._tasks[task_name] = decorated
            return decorated

        return decorator

    async def _write_job_log(
        self,
        *,
        task_name: str,
        queue: str,
        job_type: str,
        queued_by: str | None,
        status: str,
        error: str | None,
        started_at: datetime,
        finished_at: datetime,
    ) -> None:
        """`_job_log` is owned by `relay` (docs/arc.MD §3.11/§3.15) — lineup
        just inserts into it, the same way any plugin can insert into a
        table it doesn't own without needing to own its schema (ownership
        only matters for migration/diffing, §3.9). Best-effort: a DB
        hiccup writing the log row must never mask the real task's own
        outcome, which has already been decided by the time this runs."""
        try:
            await arc.psqldb.insert(
                "_job_log",
                {
                    "task_name": task_name,
                    "queue": queue,
                    "executor": "lineup",
                    "job_type": job_type,
                    "queued_by": queued_by,
                    "status": status,
                    "error": error,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
                },
            )
        except Exception as exc:
            logger.error(f"failed to write _job_log row for {task_name}: {exc}")

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
        await fn.kiq(*args, **kwargs)

    # ------------------------------------------------------------------ #
    # Ad hoc dispatch — no @task decoration, no tasks/ directory. Only the
    # function's own (module, qualname) crosses Redis, never the function
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
            raise TypeError(f"{fn!r} has no __module__/__qualname__ — not a plain importable function.")
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
        (module, qualname) arrives in a Redis message — without a check,
        anyone with write access to Redis gets arbitrary code execution in
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

        async def _dispatch(module_path: str, qualname: str, args: list, kwargs: dict) -> None:
            started_at = datetime.now(timezone.utc)
            task_name = f"{module_path}.{qualname}"
            queued_by = module_path.split(".")[0] if module_path else None
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
                await target(*args, **kwargs)
            except Exception as exc:
                status, error = "failed", f"{type(exc).__name__}: {exc}"
                raise
            finally:
                await self._write_job_log(
                    task_name=task_name,
                    queue=queue,
                    job_type="Task",
                    queued_by=queued_by,
                    status=status,
                    error=error,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                )

        self._dispatch_tasks[queue] = broker.task(task_name=f"lineup._dispatch.{queue}")(_dispatch)

    async def enqueue_by_path(self, fn: Any, *args: Any, queue: str = DEFAULT_QUEUE, **kwargs: Any) -> None:
        """Enqueue a PLAIN function — no `@arc.lineup.task(...)`/
        `@arc.relay.task(...)` decoration needed at all, callable from
        anywhere. Prefer `arc.relay.enqueue(fn, queue=..., ...)` in plugin
        code (docs/arc.MD §3.15); this exists on lineup directly mainly
        for relay's own delegation.

        Validates `fn` via check_resolvable() before ever touching Redis
        — a bad reference fails here, synchronously, not inside a worker
        process minutes or hours later."""
        module_path, qualname = self.check_resolvable(fn)
        self._broker_for(queue)  # ensures the dispatch task below actually exists
        await self._ensure_started(queue)  # a queue first touched after open() still gets a real startup()
        dispatch = self._dispatch_tasks[queue]
        await dispatch.kiq(module_path, qualname, list(args), kwargs)

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

    async def _ensure_started(self, queue: str) -> None:
        """Start a lazily-created broker if the provider is already open —
        see __init__'s lifecycle comment."""
        if self._opened and queue not in self._started:
            await self._brokers[queue].startup()
            self._started.add(queue)

    async def close(self) -> None:
        for name, broker in self._brokers.items():
            if name in self._started:
                await broker.shutdown()
        self._started.clear()
        self._opened = False

    async def health(self) -> dict:
        return {"ok": True, "queues": self.queues()}


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
