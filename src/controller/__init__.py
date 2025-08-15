from typing import Awaitable
import asyncio
import logging
import os


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
from controller.custom_workflow import workflow_controller_system
from controller.workflow_prepare_shim import get_workflow_preparer

RECONNECT_TIMEOUT = 900

API_VERSION = f"{API_GROUP}/{DEFAULT_API_VERSION}"

HOT_LOADING = True

KOREO_NAMESPACE = os.environ.get("KOREO_NAMESPACE", "koreo-testing")

TEMPLATE_NAMESPACE = os.environ.get("TEMPLATE_NAMESPACE", "koreo-testing")

RESOURCE_NAMESPACE = os.environ.get("RESOURCE_NAMESPACE", "koreo-testing")


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

logger = logging.getLogger("controller")


async def _koreo_resource_cache_manager(
    api: kr8s.asyncio.Api,
    namespace: str,
    kind_title: str,
    resource_class: type,
    preparer,
    shutdown_trigger: asyncio.Event,
):
    """
    These are long-term (infinite) cache maintainers that will run in the
    background to watch for updates to Koreo Resources.
    """
    try:
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
    finally:
        shutdown_trigger.set()


async def _controller_engine_wrapper(
    shutdown_trigger: asyncio.Event, controller: Awaitable
):
    try:
        await controller

    except KeyboardInterrupt:
        logger.debug(f"Controller engine quit due to user quit.")
        raise

    except asyncio.CancelledError:
        logger.info(f"Controller engine quit due to cancel.")
        raise

    except BaseException as err:
        logger.error(f"Controller engine quit due to error: {err}.")
        raise

    finally:
        shutdown_trigger.set()


async def controller_main(telemetry_sink: asyncio.Queue | None = None):
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

    for namespace, kind_title, resource_class, preparer in KOREO_RESOURCES:
        try:
            # Load the Koreo resources sequentially, for efficiency purposes.
            await koreo_cache.load_cache(
                api=api,
                namespace=namespace,
                api_version=API_VERSION,
                plural_kind=f"{kind_title.lower()}s",
                kind_title=kind_title,
                resource_class=resource_class,
                preparer=preparer,
            )

            # There is a trailing return
            continue

        except KeyboardInterrupt:
            logger.info(
                f"Initiating shutdown due to user-request. (Koreo {kind_title} Resource Load)"
            )

        except asyncio.CancelledError:
            logger.info(
                f"Initiating shutdown due to cancel. (Koreo {kind_title} Resource Load)"
            )

        except BaseException as err:
            logger.error(
                f"Initiating shutdown due to error {err}. (Koreo {kind_title} Resource Load)"
            )

        except:
            logger.critical(
                f"Initiating shutdown due to non-error exception. (Koreo {kind_title} Resource Load)"
            )

        # This means the continue was not hit
        return

    async with asyncio.TaskGroup() as controller_tasks:
        shutdown_trigger = asyncio.Event()

        tasks: list[asyncio.Task] = []

        if HOT_LOADING:
            logger.info("Hot-loading Koreo Resource enabled")
            for namespace, kind_title, resource_class, preparer in KOREO_RESOURCES:
                tasks.append(
                    controller_tasks.create_task(
                        _koreo_resource_cache_manager(
                            api=api,
                            namespace=namespace,
                            kind_title=kind_title,
                            resource_class=resource_class,
                            preparer=preparer,
                            shutdown_trigger=shutdown_trigger,
                        ),
                        name=f"cache-maintainer-{kind_title.lower()}",
                    )
                )

        # This is the schedule watcher / dispatcher for workflow crdRefs.
        tasks.append(
            asyncio.create_task(
                _controller_engine_wrapper(
                    shutdown_trigger=shutdown_trigger,
                    controller=workflow_controller_system(
                        api=api,
                        namespace=RESOURCE_NAMESPACE,
                        workflow_updates_queue=workflow_updates_queue,
                        telemetry_sink=telemetry_sink,
                    ),
                ),
                name="workflow-controller",
            )
        )

        try:
            await shutdown_trigger.wait()

        except KeyboardInterrupt:
            logger.info("Initiating shutdown due to user-request.")
            raise

        except SystemExit:
            logger.info("Initiating shutdown due to system exit.")
            raise

        except asyncio.CancelledError:
            logger.info("Initiating shutdown due to cancel.")
            raise

        except BaseException as err:
            logger.error(f"Initiating shutdown due to error {err}.")

        except:
            logger.critical(f"Initiating shutdown due to non-error exception.")

        logger.info("Shutting down workers")
        for task in tasks:
            task_name = task.get_name()

            if not (task.done() or task.cancelling()):
                task.cancel("System shutdown")
                logger.info(f"Stopping {task_name}")

        logger.info("Koreo controller is shutdown")
