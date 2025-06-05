from typing import Literal, NamedTuple, Protocol
import asyncio
import logging
import random

import kr8s.asyncio

from koreo.constants import PLURAL_LOOKUP_NEEDED

from controller.kind_lookup import get_full_kind

logger = logging.getLogger("koreo.controller.events")


class WatchRequest(NamedTuple):
    api_group: str
    api_version: str
    kind: str
    workflow: str


class CancelWatches(NamedTuple):
    workflow: str


class EventHandler(Protocol):
    async def __call__(
        self,
        event: Literal["ADDED", "MODIFIED", "DELETED"],
        api_group: str,
        api_version: str,
        kind: str,
        resource: kr8s._objects.APIObject,
    ): ...


class Configuration(NamedTuple):
    event_handler: EventHandler

    namespace: str

    reconnect_timeout: int = 600
    reconnect_timeout_jitter: int = 60

    missing_kind_retry_delay: int = 120

    max_unknown_errors: int = 10

    retry_delay_base: int = 30
    retry_delay_jitter: int = 30
    retry_delay_max: int = 900


def _watch_key(api_version: str, kind: str):
    return f"{kind}.{api_version}"


class StartTheWatch:
    """Used to indicate all instances of a resource should be reprocessed."""

    pass


class StopTheWatch:
    """Used to indicate a resource no longer needs watched."""

    pass


WatchQueue = asyncio.Queue[WatchRequest | CancelWatches | StopTheWatch]


async def chief_of_the_watch(
    api: kr8s.asyncio.Api,
    tg: asyncio.TaskGroup,
    watch_requests: WatchQueue,
    configuration: Configuration,
):
    watchstanders: dict[
        str, tuple[asyncio.Task, asyncio.Queue[StartTheWatch | StopTheWatch]]
    ] = {}

    # These indexes are used for watcher-task cleanup.
    workflow_watches: dict[str, str] = {}  # Workflow: Resource
    resource_watchers: dict[str, set[str]] = {}  # Resource: set(Workflow)

    while True:
        watch_request = await watch_requests.get()
        try:
            match watch_request:
                case StopTheWatch():
                    for _, queue in watchstanders.values():
                        try:
                            queue.put_nowait(StopTheWatch())
                        except (asyncio.QueueShutDown, asyncio.QueueFull):
                            pass
                    return

                case CancelWatches(workflow=workflow):
                    await _cancel_watch(
                        workflow=workflow,
                        watchstanders=watchstanders,
                        workflow_watches=workflow_watches,
                        resource_watchers=resource_watchers,
                    )

                case WatchRequest(
                    api_group=api_group,
                    api_version=api_version,
                    kind=kind,
                    workflow=workflow,
                ):
                    await _setup_watch(
                        api=api,
                        tg=tg,
                        api_group=api_group,
                        api_version=api_version,
                        kind=kind,
                        workflow=workflow,
                        watchstanders=watchstanders,
                        workflow_watches=workflow_watches,
                        resource_watchers=resource_watchers,
                        configuration=configuration,
                    )
                case _:
                    logger.error(f"Invalid watch event {watch_request}!")
        finally:
            watch_requests.task_done()


async def _cancel_watch(
    workflow: str,
    watchstanders: dict[str, tuple[asyncio.Task, asyncio.Queue]],
    workflow_watches: dict[str, str],
    resource_watchers: dict[str, set[str]],
):
    """WARNING: I mutate my arguments."""
    watched = workflow_watches.pop(workflow, None)
    if not watched:
        # This workflow has no active watches.
        return

    watchers = resource_watchers.get(watched)
    if watchers:
        watchers.remove(workflow)

    # After removing this workflow, are there any others watching?
    if not watchers:
        # No. This was the only watcher, cleanup.
        resource_watchers.pop(watched, None)

    watchstander = watchstanders.get(watched)
    if not watchstander:
        # No watchstander
        return

    _, queue = watchstander

    if watchers:
        # Watchers remain, restart in case actions are needed
        await queue.put(StartTheWatch())
        return

    # No further need for this watch.
    await queue.put(StopTheWatch())
    queue.shutdown()


async def _setup_watch(
    api: kr8s.asyncio.Api,
    tg: asyncio.TaskGroup,
    api_group: str,
    api_version: str,
    kind: str,
    workflow: str,
    watchstanders: dict[str, tuple[asyncio.Task, asyncio.Queue]],
    workflow_watches: dict[str, str],
    resource_watchers: dict[str, set[str]],
    configuration: Configuration,
):
    """WARNING: I mutate my arguments."""
    watchstander_key = _watch_key(
        api_version=f"{api_group}/{api_version}",
        kind=kind,
    )

    watched = workflow_watches.get(workflow)
    if watched and watched != watchstander_key:
        # This workflow was watching something else
        await _cancel_watch(
            workflow=workflow,
            watchstanders=watchstanders,
            workflow_watches=workflow_watches,
            resource_watchers=resource_watchers,
        )

    workflow_watches[workflow] = watchstander_key
    watchers = resource_watchers.get(watchstander_key)
    if not watchers:
        resource_watchers[watchstander_key] = set([workflow])
    else:
        watchers.add(workflow)

    existing = watchstanders.get(watchstander_key)
    if existing:
        # There is already a watch on this resource. Restart it.
        _, watchstander_queue = existing
        await watchstander_queue.put(StartTheWatch())
        return

    watchstander_queue = asyncio.Queue()
    watchstander_task = tg.create_task(
        _watchstander_task(
            api=api,
            api_group=api_group,
            version=api_version,
            kind=kind,
            command_queue=watchstander_queue,
            configuration=configuration,
        ),
        name=f"watchstander-{watchstander_key}",
    )
    watchstanders[watchstander_key] = (
        watchstander_task,
        watchstander_queue,
    )

    def done(task):
        logger.debug(f"Watch task {task.get_name()} is done")
        watchstanders.pop(watchstander_key)

    watchstander_task.add_done_callback(done)


async def _watchstander_task(
    api: kr8s.asyncio.Api,
    api_group: str,
    version: str,
    kind: str,
    command_queue: asyncio.Queue,
    configuration: Configuration,
):
    name = f"{kind}.{api_group}.{version}"
    restarts = 0
    error_restarts = 0
    while True:
        # TODO: Use resource for setup, resource should include workflow id
        # TODO: Race with restart to replay the wathc on workflow updates
        watchstander_task = asyncio.create_task(
            _watchstander(
                api=api,
                api_group=api_group,
                version=version,
                kind_title=kind,
                configuration=configuration,
            ),
            name=f"watchstander-{name}-{restarts}-{error_restarts}",
        )
        restarts += 1

        command_queue_task = asyncio.create_task(
            command_queue.get(),
            name=f"command-queue-{name}-{restarts}-{error_restarts}",
        )

        await asyncio.wait(
            [watchstander_task, command_queue_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if watchstander_task.done():
            if not command_queue_task.done():
                command_queue_task.cancel()

            if err := watchstander_task.exception():
                if isinstance(err, (asyncio.CancelledError, KeyboardInterrupt)):
                    raise err

                delay = min(
                    (2**error_restarts) * configuration.retry_delay_base
                    + random.randint(0, configuration.retry_delay_jitter),
                    configuration.retry_delay_max,
                )

                error_restarts += 1
                if error_restarts < configuration.max_unknown_errors:
                    logger.info(
                        f"Waiting {delay} seconds before restarting the watch "
                        f"for {name} due to error '{err}'"
                    )

                    await asyncio.sleep(delay)

                    continue

                logger.error(
                    f"Too many errors watching `{name}`. Aborting due to `{err}`."
                )
                return

        if command_queue_task.done():
            if not watchstander_task.done():
                watchstander_task.cancel()

            if not command_queue_task.cancelled():
                match command_queue_task.result():
                    case StopTheWatch():
                        logger.debug(f"Stopping the watch for {name}")
                        return

                    case StartTheWatch():
                        # This is used to force the watchstander to restart.
                        pass

        error_restarts = 0


async def _watchstander(
    api: kr8s.asyncio.Api,
    api_group: str,
    version: str,
    kind_title: str,
    configuration: Configuration,
):
    api_version = f"{api_group}/{version}"

    full_kind = await get_full_kind(api=api, kind=kind_title, api_version=api_version)

    if not full_kind:
        # QUESTION: Anything better to do here?
        logger.warning(
            f"Failed to find plural kind for `{kind_title}` in api group `{api_version}`."
        )
        await asyncio.sleep(configuration.missing_kind_retry_delay)
        return

    logger.info(f"Controller starting for `{full_kind}`.")

    try:
        k8s_object_class = kr8s.objects.get_class(
            kind=f"{kind_title}.{api_version}", _asyncio=True
        )

        if k8s_object_class.plural == PLURAL_LOOKUP_NEEDED:
            (plural, _) = full_kind.split(".", 1)
            logger.debug(f"Setting {kind_title}'s API Object plural to '{plural}'")
            k8s_object_class.plural = plural
            k8s_object_class.endpoint = plural

    except:
        k8s_object_class = full_kind

    try:
        watcher = api.async_watch(
            kind=k8s_object_class, namespace=configuration.namespace
        )

        async with asyncio.timeout(configuration.reconnect_timeout):
            async for event, resource in watcher:
                logger.debug(
                    f"Handling {event} for `{full_kind}` object `{resource.name}`."
                )
                await configuration.event_handler(
                    event=event,
                    api_group=api_group,
                    api_version=version,
                    kind=kind_title,
                    resource=resource,
                )

    except asyncio.CancelledError:
        logger.debug(f"Cancelling `{full_kind}` watch.")
        raise
    except KeyboardInterrupt:
        logger.debug(f"Shutdown `{full_kind}` watch.")
        raise

    except asyncio.TimeoutError:
        logger.debug(f"Restarting `{full_kind}` watch due to timeout.")
        # This is just to reduce some load on the API server if there's issue
        await asyncio.sleep(
            random.randint(15, max(30, configuration.reconnect_timeout_jitter))
        )
        return
