import asyncio
import logging
import os

import kr8s.asyncio

from controller import events
from controller import scheduler
from controller import reconcile

logger = logging.getLogger("koreo.controller.reconcile")

RECONNECT_TIMEOUT = 900

MANAGED_RESOURCE_API_SERVER_URL = os.environ.get("MANAGED_RESOURCE_API_SERVER_URL")
MANAGED_RESOURCE_KUBECONFIG = os.environ.get("MANAGED_RESOURCE_KUBECONFIG")
MANAGED_RESOURCE_CONTEXT = os.environ.get("MANAGED_RESOURCE_CONTEXT")
MANAGED_RESOURCE_SERVICEACCOUNT = os.environ.get("MANAGED_RESOURCE_SERVICEACCOUNT")
MANAGED_RESOURCE_NAMESPACE = os.environ.get("MANAGED_RESOURCE_NAMESPACE")


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
    telemetry_sink: asyncio.Queue | None = None,
):
    event_handler, request_queue = reconcile.get_event_handler(namespace=namespace)

    event_config = events.Configuration(
        event_handler=event_handler,
        telemetry_sink=telemetry_sink,
        namespace=namespace,
        max_unknown_errors=10,
        retry_delay_base=30,
        retry_delay_jitter=30,
        retry_delay_max=900,
    )

    if not (
        MANAGED_RESOURCE_API_SERVER_URL
        or MANAGED_RESOURCE_KUBECONFIG
        or MANAGED_RESOURCE_CONTEXT
        or MANAGED_RESOURCE_SERVICEACCOUNT
    ):
        logger.debug(f"Managing resources in controller cluster")
        managed_resource_api = api
    else:
        if MANAGED_RESOURCE_NAMESPACE:
            managed_resource_namespace = MANAGED_RESOURCE_NAMESPACE
        else:
            managed_resource_namespace = kr8s.ALL

        logger.info(f"Managing resources in remote cluster")

        try:
            managed_resource_api = await kr8s.asyncio.api(
                url=MANAGED_RESOURCE_API_SERVER_URL,
                kubeconfig=MANAGED_RESOURCE_KUBECONFIG,
                context=MANAGED_RESOURCE_CONTEXT,
                serviceaccount=MANAGED_RESOURCE_SERVICEACCOUNT,
                namespace=managed_resource_namespace,
            )
        except BaseException as err:
            logger.error(f"Failed to create remote cluster api with {err}")
            raise

        try:
            logger.info(
                f"Remote cluster version: {await managed_resource_api.version()}"
            )
            logger.info(
                f"Remote cluster subject: {await managed_resource_api.whoami()}"
            )
        except BaseException as err:
            logger.error(f"Failed to log remote cluster api info {err}")
            raise

        managed_resource_api.timeout = RECONNECT_TIMEOUT

    scheduler_config = scheduler.Configuration(
        concurrency=2,
        frequency_seconds=1200,
        retry_delay_base=30,
        retry_delay_max=900,
        work_processor=_configure_reconciler(api=managed_resource_api),
        telemetry_sink=telemetry_sink,
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
