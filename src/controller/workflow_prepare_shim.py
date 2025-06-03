import asyncio
import time
import logging

logger = logging.getLogger("koreo.workflow")

from koreo import registry
from koreo.cache import get_resource_from_cache
from koreo.result import is_unwrapped_ok
from koreo.workflow import prepare
from koreo.workflow.structure import Workflow

from controller.events import WatchQueue, CancelWatches, WatchRequest
from controller.workflow_registry import (
    index_workflow_custom_crd,
    unindex_workflow_custom_crd,
)

_WORKFLOW_RECONCILERS: dict[str, asyncio.Task] = {}
_DEREGISTERERS: dict[str, asyncio.Task] = {}


def get_workflow_preparer():
    workflow_update_queue = WatchQueue()

    async def prepare_workflow(cache_key: str, spec: dict):
        logger.info(f"Preparing workflow handler from cache key: {cache_key}")
        prepare_result = await prepare.prepare_workflow(cache_key=cache_key, spec=spec)

        if not is_unwrapped_ok(prepare_result):
            logger.warning(f"Failed to prepare workflow from cache key: {cache_key}")
            return prepare_result

        workflow, watched = prepare_result
        logger.info(f"Found workflow handler from cache key: {cache_key}")

        await _workflow_post_prepare(
            workflow_update_queue=workflow_update_queue,
            cache_key=cache_key,
            workflow=workflow,
        )

        logger.info(f"Workflow handler registered: {workflow.name}")

        return workflow, watched

    return prepare_workflow, workflow_update_queue


async def _workflow_post_prepare(
    workflow_update_queue: WatchQueue, cache_key: str, workflow: Workflow
):
    deletor_name = f"DeleteWorkflow:{cache_key}"
    workflow_reconciler_name = f"WorkflowReconciler:{cache_key}"

    if not workflow.crd_ref or not is_unwrapped_ok(workflow):
        unindex_workflow_custom_crd(workflow=cache_key)

        if deletor_name in _DEREGISTERERS:
            # TODO: Is more needed here?
            _DEREGISTERERS[deletor_name].cancel()
            _WORKFLOW_RECONCILERS[workflow_reconciler_name].cancel()
        return

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

    if workflow_reconciler_name not in _WORKFLOW_RECONCILERS:
        await workflow_update_queue.put(
            WatchRequest(
                api_group=crd_ref.api_group,
                api_version=crd_ref.version,
                kind=crd_ref.kind,
                workflow=cache_key,
            )
        )
        workflow_reconciler_task = asyncio.create_task(
            _workflow_reconciler(
                cache_key=cache_key, workflow_update_queue=workflow_update_queue
            ),
            name=workflow_reconciler_name,
        )
        _WORKFLOW_RECONCILERS[workflow_reconciler_name] = workflow_reconciler_task
        workflow_reconciler_task.add_done_callback(
            lambda task: _WORKFLOW_RECONCILERS.__delitem__(task.get_name())
        )


class WorkflowReconciler: ...


async def _workflow_reconciler(workflow_update_queue: WatchQueue, cache_key: str):
    workflow_reconciler_resource = registry.Resource(
        resource_type=WorkflowReconciler, name=cache_key, namespace=None
    )
    queue = registry.register(workflow_reconciler_resource)

    registry.subscribe(
        subscriber=workflow_reconciler_resource,
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

                    if not cached or not is_unwrapped_ok(cached) or not cached.crd_ref:
                        logger.error(f"Cancel CRD watches for Workflow {cache_key}")
                        break

                    await workflow_update_queue.put(
                        WatchRequest(
                            api_group=cached.crd_ref.api_group,
                            api_version=cached.crd_ref.version,
                            kind=cached.crd_ref.kind,
                            workflow=cache_key,
                        )
                    )

                    continue

        finally:
            queue.task_done()

    registry.deregister(workflow_reconciler_resource, deregistered_at=time.monotonic())
    await workflow_update_queue.put(CancelWatches(workflow=cache_key))


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
