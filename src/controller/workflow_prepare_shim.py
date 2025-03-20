import asyncio
import time
import logging

logger = logging.getLogger("koreo.workflow")

from koreo import registry
from koreo.cache import get_resource_from_cache
from koreo.result import is_unwrapped_ok
from koreo.workflow import prepare
from koreo.workflow.structure import Workflow

from controller.custom_workflow import start_controller
from controller.workflow_registry import (
    index_workflow_custom_crd,
    unindex_workflow_custom_crd,
)

_DEREGISTERERS: dict[str, asyncio.Task] = {}


async def prepare_workflow(cache_key: str, spec: dict):
    prepare_result = await prepare.prepare_workflow(cache_key=cache_key, spec=spec)

    if not is_unwrapped_ok(prepare_result):
        return prepare_result

    workflow, watched = prepare_result

    _workflow_post_prepare(cache_key=cache_key, workflow=workflow)

    return workflow, watched


def _workflow_post_prepare(cache_key: str, workflow: Workflow):
    if not workflow.crd_ref or not is_unwrapped_ok(workflow):
        unindex_workflow_custom_crd(workflow=cache_key)
        return

    deletor_name = f"DeleteWorkflow:{cache_key}"
    if deletor_name not in _DEREGISTERERS:
        delete_task = asyncio.create_task(
            _deindex_crd_on_delete(cache_key=cache_key), name=deletor_name
        )
        _DEREGISTERERS[deletor_name] = delete_task
        delete_task.add_done_callback(
            lambda task: _DEREGISTERERS.__delitem__(task.get_name())
        )

    crd_ref = workflow.crd_ref
    index_workflow_custom_crd(
        workflow=cache_key,
        custom_crd=f"{crd_ref.api_group}:{crd_ref.kind}:{crd_ref.version}",
    )

    start_controller(
        group=crd_ref.api_group, kind=crd_ref.kind, version=crd_ref.version
    )


class WorkflowDeleteor: ...


async def _deindex_crd_on_delete(cache_key: str):
    deletor_resource = registry.Resource(
        resource_type=WorkflowDeleteor, name=cache_key, namespace=None
    )
    queue = registry.register(deletor_resource)

    registry.subscribe(
        subscriber=deletor_resource,
        resource=registry.Resource(
            resource_type=Workflow, name=cache_key, namespace=None
        ),
    )

    last_event = 0
    while True:
        try:
            event = await queue.get()
        except (asyncio.CancelledError, asyncio.QueueShutDown):
            break

        try:
            match event:
                case registry.Kill():
                    break
                case registry.ResourceEvent(event_time=event_time) if (
                    event_time >= last_event
                ):
                    cached = get_resource_from_cache(
                        resource_class=Workflow, cache_key=cache_key
                    )

                    if cached:
                        continue

                    logger.debug(f"Deregistering CRD watches for Workflow {cache_key}")

                    unindex_workflow_custom_crd(workflow=cache_key)

                    break

        finally:
            queue.task_done()

    registry.deregister(deletor_resource, deregistered_at=time.monotonic())
