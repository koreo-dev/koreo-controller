import asyncio
import logging

import kr8s.asyncio

from controller import events
from controller import scheduler
from controller import reconcile

logger = logging.getLogger("koreo.controller.reconcile")


def _configure_reconciler(
    api: kr8s.asyncio.Api,
):
    async def wrapped(
        payload: reconcile.Resource,
        ok_frequency_seconds: int,
        sys_error_retries: int,
        user_retries: int,
    ):
        try:
            return await reconcile.reconcile_resource(
                api=api,
                payload=payload,
                ok_frequency_seconds=ok_frequency_seconds,
                sys_error_retries=sys_error_retries,
                user_retries=user_retries,
            )
        except Exception as err:
            logger.exception(f"Error reconciling resource '{err}'")

    return wrapped


async def workflow_controller_system(
    api: kr8s.asyncio.Api,
    namespace: str,
    workflow_updates_queue: events.WatchQueue,
):
    event_handler, request_queue = reconcile.get_event_handler(namespace=namespace)

    event_config = events.Configuration(
        event_handler=event_handler, namespace=namespace
    )

    scheduler_config = scheduler.Configuration(
        work_processor=_configure_reconciler(api=api)
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
