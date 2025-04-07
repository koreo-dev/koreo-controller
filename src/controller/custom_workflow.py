import asyncio

import kr8s.asyncio

from controller import events
from controller import scheduler
from controller import reconcile


async def workflow_controller_system(
    namespace: str,
    workflow_updates_queue: events.WatchQueue,
):
    api = await kr8s.asyncio.api()

    event_handler, request_queue = reconcile.get_event_handler(namespace=namespace)

    event_config = events.Configuration(
        event_handler=event_handler, namespace=namespace
    )

    scheduler_config = scheduler.Configuration(
        work_processor=reconcile.reconcile_resource
    )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            events.chief_of_the_watch(
                api=api,
                tg=tg,
                watch_requests=workflow_updates_queue,
                configuration=event_config,
            ),
            name="workflow-chief-of-the-watch",
        )

        tg.create_task(
            scheduler.orchestrator(
                tg=tg, requests=request_queue, configuration=scheduler_config
            ),
            name="workflow-reconcile-scheduler",
        )
