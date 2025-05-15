import json
import logging
import os

import asyncio


class JsonFormatter(logging.Formatter):
    def format(self, record):
        record_dict = {
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
            "time": self.formatTime(record),
        }
        return json.dumps(record_dict)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())

logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", logging.INFO)
logger.setLevel(log_level)
logger.addHandler(handler)

lib_log_level = os.getenv("LIB_LOG_LEVEL", logging.WARNING)
logging.getLogger(name="httpcore.connection").setLevel(lib_log_level)
logging.getLogger(name="httpcore.http11").setLevel(lib_log_level)
logging.getLogger(name="httpx").setLevel(lib_log_level)
logging.getLogger(name="kr8s._api").setLevel(lib_log_level)
logging.getLogger(name="kr8s._auth").setLevel(lib_log_level)

import os

import uvloop

import kr8s.asyncio

from koreo.constants import API_GROUP, DEFAULT_API_VERSION
from koreo.resource_function.prepare import prepare_resource_function
from koreo.resource_function.structure import ResourceFunction
from koreo.resource_template.prepare import prepare_resource_template
from koreo.resource_template.structure import ResourceTemplate
from koreo.value_function.prepare import prepare_value_function
from koreo.value_function.structure import ValueFunction
from koreo.workflow.structure import Workflow

from controller import koreo_cache
from controller import load_schemas
from controller.workflow_prepare_shim import get_workflow_preparer
from controller.custom_workflow import workflow_controller_system

RECONNECT_TIMEOUT = 900

API_VERSION = f"{API_GROUP}/{DEFAULT_API_VERSION}"

HOT_LOADING = True

KOREO_NAMESPACE = os.environ.get("KOREO_NAMESPACE", "koreo-testing")

TEMPLATE_NAMESPACE = os.environ.get("TEMPLATE_NAMESPACE", "koreo-testing")

RESOURCE_NAMESPACE = os.environ.get("RESOURCE_NAMESPACE", "koreo-testing")


EXIT_CONTROLLER_ERROR = 1
EXIT_CONTROLLER_UNEXPECTED_RETURN = 2

EXIT_CACHER_ERROR = 5
EXIT_CACHER_UNEXPECTED_RETURN = 6


# NOTE: These are ordered so that each group's dependencies will already be
# loaded when initially loaded into cache.
KOREO_RESOURCES = [
    (
        TEMPLATE_NAMESPACE,
        "ResourceTemplate",
        ResourceTemplate,
        prepare_resource_template,
    ),
    (KOREO_NAMESPACE, "ValueFunction", ValueFunction, prepare_value_function),
    (KOREO_NAMESPACE, "ResourceFunction", ResourceFunction, prepare_resource_function),
    # NOTE: Workflow is appended within `main` to integrate updates queue.
]


async def _koreo_resource_cache_manager(
    api: kr8s.asyncio.Api,
    namespace: str,
    kind_title: str,
    resource_class: type,
    preparer,
):
    # Block until completion.
    await koreo_cache.load_cache(
        api=api,
        namespace=namespace,
        api_version=API_VERSION,
        plural_kind=f"{kind_title.lower()}s",
        kind_title=kind_title,
        resource_class=resource_class,
        preparer=preparer,
    )

    if not HOT_LOADING:
        return

    # Spawns long-term (infinite) cache maintainer in background
    await koreo_cache.maintain_cache(
        api=api,
        namespace=namespace,
        api_version=API_VERSION,
        plural_kind=f"{kind_title.lower()}s",
        kind_title=kind_title,
        resource_class=resource_class,
        preparer=preparer,
        reconnect_timeout=RECONNECT_TIMEOUT,
    )


def _cache_task_complete(cache_task: asyncio.Task):
    if cache_task.cancelled():
        logger.info(f"Cache task ({cache_task.get_name()}) quit due to cancel.")
        return

    if cache_task.exception():
        logger.error(
            f"Cache task ({cache_task.get_name()}) quit due to error: {cache_task.exception()}."
        )
        raise SystemExit(EXIT_CACHER_ERROR)

    if not HOT_LOADING:
        return

    logger.error(f"Cache task ({cache_task.get_name()}) quit due to unexpected return.")
    raise SystemExit(EXIT_CACHER_UNEXPECTED_RETURN)


def _controller_engine_complete(controller_task: asyncio.Task):
    if controller_task.cancelled():
        logger.info("Controller engine quit due to cancel.")
        return

    if controller_task.exception():
        logger.error(
            f"Controller engine quit due to error: {controller_task.exception()}."
        )
        raise SystemExit(EXIT_CONTROLLER_ERROR)

    logger.error(f"Controller engine quit due to unexpected return.")
    raise SystemExit(EXIT_CONTROLLER_UNEXPECTED_RETURN)


async def main():
    logger.info("Koreo Controller Starting")

    api = await kr8s.asyncio.api()
    api.timeout = RECONNECT_TIMEOUT

    # The schemas must be loaded before Koreo resources can be prepared.
    await load_schemas.load_koreo_resource_schemas(api)

    # This is so the resources can be re-reconciled if their Workflows are
    # updated.
    prepare_workflow, workflow_updates_queue = get_workflow_preparer()

    KOREO_RESOURCES.append(
        (KOREO_NAMESPACE, "Workflow", Workflow, prepare_workflow),
    )

    async with asyncio.TaskGroup() as main_tg:
        for namespace, kind_title, resource_class, preparer in KOREO_RESOURCES:
            cache_task = main_tg.create_task(
                _koreo_resource_cache_manager(
                    api=api,
                    namespace=namespace,
                    kind_title=kind_title,
                    resource_class=resource_class,
                    preparer=preparer,
                ),
                name=f"cache-maintainer-{kind_title.lower()}",
            )
            cache_task.add_done_callback(_cache_task_complete)

        # This is the schedule watcher / dispatcher for workflow crdRefs.
        orchestrator_task = asyncio.create_task(
            workflow_controller_system(
                api=api,
                namespace=RESOURCE_NAMESPACE,
                workflow_updates_queue=workflow_updates_queue,
            )
        )
        orchestrator_task.add_done_callback(_controller_engine_complete)


if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    try:
        uvloop.run(main())
    except (asyncio.CancelledError, KeyboardInterrupt):
        exit(0)
