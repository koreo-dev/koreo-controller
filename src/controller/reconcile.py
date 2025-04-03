from enum import StrEnum
from typing import Awaitable, Callable, NamedTuple
import hashlib
import json
import logging
import time

from celpy import celtypes
import celpy
import kr8s.asyncio

from koreo.cache import get_resource_system_data_from_cache
from koreo.constants import PREFIX
from koreo.cel.encoder import convert_bools
from koreo.conditions import Condition, update_condition
from koreo.result import Ok, Outcome, PermFail, Retry, is_error, is_unwrapped_ok
from koreo.workflow.reconcile import reconcile_workflow
from koreo.workflow.structure import Workflow

from controller.workflow_registry import get_custom_crd_workflows

from controller import scheduler

logger = logging.getLogger("koreo.controller.reconcile")

MISSING_WORKFLOW_RETRY = 120


class AnnotationKeys(StrEnum):
    managed_resources = f"{PREFIX}/managed-resources"
    workflow = f"{PREFIX}/workflow"
    merge_conditions = f"{PREFIX}/merge-conditions"


class Resource(NamedTuple):
    group: str
    version: str
    kind: str
    name: str
    namespace: str


class LastReconcile(NamedTuple):
    at: float
    next_at: float

    workflow_id: str | None = None

    outcome: Outcome[float] | None = None

    sys_error_retries: int = 0
    user_retries: int = 0


class CachedResource(NamedTuple):
    resource_hash: str
    cached: kr8s._objects.APIObject | None = None
    last_reconcile: LastReconcile | None = None


class DeletedTombstone(NamedTuple):
    uid: str


__resource_cache: dict[Resource, CachedResource | DeletedTombstone] = {}


def get_event_handler(namespace: str):
    request_queue = scheduler.RequestQueue[Resource]()

    async def event_handler(
        event: str,
        api_group: str,
        api_version: str,
        kind: str,
        resource: kr8s._objects.APIObject,
    ):
        resource_key = Resource(
            group=api_group,
            version=api_version,
            kind=kind,
            name=resource.name,
            namespace=namespace,
        )
        old_cache = __resource_cache.get(resource_key)

        resource_uid = resource.raw.get("metadata", {}).get("uid")

        if event == "DELETED":
            if not old_cache:
                return

            __resource_cache[resource_key] = DeletedTombstone(uid=resource_uid)
            return

        resource_hash = _hash_resource(resource.raw)

        if isinstance(old_cache, DeletedTombstone):
            if old_cache.uid == resource_uid:
                # NoOp; stale event for a deleted resource.
                return

            old_cache = None

        if not old_cache or old_cache.resource_hash != resource_hash:
            __resource_cache[resource_key] = CachedResource(
                resource_hash=resource_hash, cached=resource
            )

        await request_queue.put(
            scheduler.Request(at=time.monotonic(), payload=resource_key)
        )

    return event_handler, request_queue


def _hash_resource(resource: dict):
    """
    Compute a hash of the resource to determine if anything is different since
    the last reconcile. This hash is computed using the full spec, plus
    metadata labels and annotations (excluding koreo annotations).
    """
    metadata = resource.get("metadata", {})
    spec = resource.get("spec", {})

    non_koreo_annotations = metadata.get("annotations")
    if non_koreo_annotations:
        non_koreo_annotations = {
            key: value
            for key, value in non_koreo_annotations.items()
            if not key.startswith(PREFIX)
        }

    hash_worthy_resource = {
        "metadata": {
            "labels": metadata.get("labels"),
            "annotations": non_koreo_annotations,
        },
        "spec": spec,
    }

    return hashlib.sha256(
        json.dumps(hash_worthy_resource, sort_keys=True).encode("utf-8")
    ).hexdigest()


async def reconcile_resource(
    payload: Resource,
    ok_frequency_seconds: int,
    sys_error_retries: int,
    user_retries: int,
):
    api = await kr8s.asyncio.api()

    cached_resource = __resource_cache.get(payload)
    if not cached_resource:
        # TODO: Attempt to load from cluster?
        logger.error(f"Failed to find resource in cache ({payload}).")
        return None

    if isinstance(cached_resource, DeletedTombstone):
        logger.debug(f"{payload} was deleted from cluster.")
        return None

    if not cached_resource.cached:
        # TODO: Attempt to load from cluster?
        logger.error(f"Missing cached resource ({payload}).")
        return None

    cached_metadata = cached_resource.cached.raw.get("metadata", {})

    if not cached_metadata:
        # TODO: Attempt to load from cluster?
        logger.error(f"Corrupt cached resource is missing metadata ({payload}).")
        return None

    merge_conditions = (
        cached_metadata.get("annotations", {}).get(
            AnnotationKeys.merge_conditions, "false"
        )
        == "true"
    )

    if merge_conditions:
        conditions = cached_resource.cached.raw.get("status", {}).get("conditions", [])
    else:
        conditions: list[Condition] = []

    match _lookup_workflow_for_resource(
        resource=payload,
        annotations=cached_metadata.get("annotations", {}),
        conditions=conditions,
    ):
        case WorkflowLookupError(
            message=message, result=result, patch_value=patch_value
        ):
            logger.warning(f"Workflow lookup error: {message}")
            await cached_resource.cached.async_patch(patch_value)
            return result
        case LoadedWorkflow(workflow=workflow, version=workflow_version):
            # The workflow is used later as well.
            workflow_id = f"{workflow.name}:{workflow_version}"

    cache_check = __resource_cache.get(payload)
    if not cache_check:
        # TODO: What could cause this?
        logger.error(
            f"Failed load check resource from cache ({payload}) (after loading workflow)."
        )
        return None

    if isinstance(cache_check, DeletedTombstone):
        logger.debug(f"{payload} was deleted from cluster while loading workflow.")
        return None

    if cache_check.resource_hash != cached_resource.resource_hash:
        # The update should already have inserted a new reconcile request.
        logger.info(
            f"Aborting reconcile because {payload} updated while looking up workflow."
        )
        return None

    if (
        cache_check.last_reconcile  # It has been reconciled.
        and cache_check.last_reconcile.workflow_id == workflow_id  # by this workflow
    ):
        if is_error(cache_check.last_reconcile.outcome):
            if isinstance(cache_check.last_reconcile.outcome, PermFail):
                # PermFail should wait for a resource or workflow change.
                logger.info(
                    f"{payload} in PermFail state, will not reattempt "
                    f"without update to resource or workflow ({workflow.name})."
                )
                return None
            # Retry will flow through.

        elif time.monotonic() < cache_check.last_reconcile.next_at - 5:
            seconds_to_wait = round(
                cache_check.last_reconcile.next_at - time.monotonic()
            )
            logger.debug(
                f"{payload} reconciled `{cache_check.last_reconcile.outcome}`, "
                f"not ready for a re-reconcile for {seconds_to_wait} seconds."
            )
            return None

    owner = (
        f"{cached_resource.cached.namespace}",
        {
            "apiVersion": f"{payload.group}/{payload.version}",
            "kind": payload.kind,
            "blockOwnerDeletion": True,
            "controller": False,
            "name": cached_metadata.get("name"),
            "uid": cached_metadata.get("uid"),
        },
    )

    cached_spec = cached_resource.cached.raw.get("spec")
    cached_state = cached_resource.cached.raw.get("status", {}).get("state")

    trigger = celpy.json_to_cel(
        {"metadata": cached_metadata, "spec": cached_spec, "state": cached_state}
    )

    logger.info(f"Running Workflow {workflow.name} to reconcile {payload}.")

    reconcile_outcome = await reconcile_with_workflow(
        api=api,
        workflow=workflow,
        owner=owner,
        trigger=trigger,
        conditions=conditions,
        patch=cached_resource.cached.async_patch,
    )

    cache_check = __resource_cache.get(payload)
    if not cache_check:
        # TODO: What could cause this?
        logger.error(
            f"Failed load check resource from cache ({payload}) after reconciling."
        )
        return None

    if isinstance(cache_check, DeletedTombstone):
        logger.warning(f"{payload} was deleted from cluster while reconciling.")
        return None

    if cache_check.resource_hash != cached_resource.resource_hash:
        # The update should already have inserted a new reconcile request.
        logger.info(
            f"Skipping cache and conditions update because {payload} was "
            "updated while reconciling."
        )
        return None

    __resource_cache[payload] = CachedResource(
        resource_hash=cache_check.resource_hash,
        cached=cache_check.cached,
        last_reconcile=LastReconcile(
            at=time.monotonic(),
            next_at=time.monotonic() + ok_frequency_seconds,
            workflow_id=workflow_id,
            outcome=reconcile_outcome,
            sys_error_retries=sys_error_retries,
            user_retries=user_retries,
        ),
    )

    cached_resource = __resource_cache.get(payload)

    return reconcile_outcome


class LoadedWorkflow(NamedTuple):
    workflow: Workflow
    version: str


class WorkflowLookupError(NamedTuple):
    message: str
    result: Retry
    patch_value: dict


def _lookup_workflow_for_resource(
    resource: Resource,
    annotations: dict[str, str],
    conditions: list[Condition] | None,
) -> LoadedWorkflow | WorkflowLookupError:
    crd_key = f"{resource.group}:{resource.kind}:{resource.version}"

    user_specified_workflow = annotations.get(AnnotationKeys.workflow)
    if user_specified_workflow:
        logger.info(
            f"Looking up user-specified workflow ({user_specified_workflow}) for `{crd_key}`"
        )
        cached_workflow = get_resource_system_data_from_cache(
            resource_class=Workflow, cache_key=user_specified_workflow
        )

        if not cached_workflow:
            message = f"Failed to find User Requested Workflow ({user_specified_workflow}) for `{crd_key}`"
            condition = Condition(
                type="Ready",
                reason="NoWorkflow",
                message=message,
                status="false",
                location="custom_workflow._lookup_workflow_for_resource",
            )

            return WorkflowLookupError(
                message=message,
                result=Retry(message=message, delay=MISSING_WORKFLOW_RETRY),
                patch_value={
                    "status": {
                        "conditions": update_condition(
                            conditions=conditions, condition=condition
                        ),
                        "koreo": {
                            "errors": message,
                            "locations": f"custom_workflow._lookup_workflow_for_resource({crd_key})",
                        },
                    }
                },
            )

        if is_unwrapped_ok(cached_workflow.resource):
            return LoadedWorkflow(
                workflow=cached_workflow.resource,
                version=cached_workflow.resource_version,
            )

        message = f"User-specified Workflow `{user_specified_workflow}` not ready {cached_workflow.resource.message}."
        condition = Condition(
            type="Ready",
            reason="WorkflowNotReady",
            message=message,
            status="false",
            location="custom_workflow._lookup_workflow_for_resource<user-specified>",
        )

        return WorkflowLookupError(
            message=message,
            result=Retry(message=message, delay=MISSING_WORKFLOW_RETRY),
            patch_value={
                "status": {
                    "conditions": update_condition(
                        conditions=conditions, condition=condition
                    ),
                    "koreo": {
                        "errors": message,
                        "locations": "custom_workflow._lookup_workflow_for_resource<user-specified>",
                    },
                }
            },
        )

    logger.info(f"Looking up workflow(s) for {crd_key}")
    workflow_keys = get_custom_crd_workflows(custom_crd=crd_key)
    if not workflow_keys:
        message = f"Failed to find Workflow for `{crd_key}`"
        condition = Condition(
            type="Ready",
            reason="NoWorkflow",
            message=message,
            status="false",
            location="custom_workflow._lookup_workflow_for_resource",
        )

        return WorkflowLookupError(
            message=message,
            result=Retry(message=message, delay=MISSING_WORKFLOW_RETRY),
            patch_value={
                "status": {
                    "conditions": update_condition(
                        conditions=conditions, condition=condition
                    ),
                    "koreo": {
                        "errors": message,
                        "locations": f"custom_workflow.reconcile({crd_key})",
                    },
                }
            },
        )

    if len(workflow_keys) > 1:
        message = f"Multiple Workflows attempted to run ({','.join(workflow_keys)})"
        condition = Condition(
            type="Ready",
            reason="MultipleWorkflows",
            message=message,
            status="false",
            location=f"{';'.join(workflow_keys)}",
        )

        return WorkflowLookupError(
            message=message,
            result=Retry(message=message, delay=MISSING_WORKFLOW_RETRY),
            patch_value={
                "status": {
                    "conditions": update_condition(
                        conditions=conditions, condition=condition
                    ),
                    "koreo": {
                        "errors": message,
                        "locations": f"{';'.join(workflow_keys)}",
                    },
                }
            },
        )

    workflow_key, *_ = workflow_keys

    cached_workflow = get_resource_system_data_from_cache(
        resource_class=Workflow, cache_key=workflow_key
    )

    if not cached_workflow:
        message = f"Could not load Workflow `{workflow_key}`."
        condition = Condition(
            type="Ready",
            reason="WorkflowNotInCache",
            message=message,
            status="false",
            location="custom_workflow._lookup_workflow_for_resource",
        )

        return WorkflowLookupError(
            message=message,
            result=Retry(message=message, delay=MISSING_WORKFLOW_RETRY),
            patch_value={
                "status": {
                    "conditions": update_condition(
                        conditions=conditions, condition=condition
                    ),
                    "koreo": {
                        "errors": message,
                        "locations": "custom_workflow._lookup_workflow_for_resource",
                    },
                }
            },
        )

    if not is_unwrapped_ok(cached_workflow.resource):
        message = (
            f"Workflow `{workflow_key}` not ready {cached_workflow.resource.message}."
        )
        condition = Condition(
            type="Ready",
            reason="WorkflowNotReady",
            message=message,
            status="false",
            location="custom_workflow._lookup_workflow_for_resource",
        )

        return WorkflowLookupError(
            message=message,
            result=Retry(message=message, delay=MISSING_WORKFLOW_RETRY),
            patch_value={
                "status": {
                    "conditions": update_condition(
                        conditions=conditions, condition=condition
                    ),
                    "koreo": {
                        "errors": message,
                        "locations": "custom_workflow._lookup_workflow_for_resource",
                    },
                }
            },
        )

    return LoadedWorkflow(
        workflow=cached_workflow.resource, version=cached_workflow.resource_version
    )


async def reconcile_with_workflow(
    api: kr8s.asyncio.Api,
    workflow: Workflow,
    owner: tuple[str, dict],
    trigger: celtypes.Value,
    conditions: list[Condition],
    patch: Callable[..., Awaitable],
):
    workflow_result = await reconcile_workflow(
        api=api,
        workflow_key=workflow.name,
        owner=owner,
        trigger=trigger,
        workflow=workflow,
    )

    outcome = workflow_result.result
    resource_ids = workflow_result.resource_ids
    state = workflow_result.state
    state_errors = workflow_result.state_errors

    for condition in workflow_result.conditions:
        conditions = update_condition(conditions=conditions, condition=condition)

    encoded_resource_ids = json.dumps(resource_ids, separators=(",", ":"), indent=None)

    object_patch = {
        "metadata": {
            "annotations": {AnnotationKeys.managed_resources: encoded_resource_ids}
        },
        "status": {
            "conditions": conditions,
            "state": convert_bools(state),
        },
    }

    if is_error(outcome):
        object_patch["status"]["koreo"] = {
            "errors": outcome.message,
            "locations": outcome.location,
            "state_errors": state_errors if state_errors else None,
        }
        await patch(object_patch)

        return outcome

    koreo_value = {
        "errors": None,
        "locations": None,
        "state_errors": state_errors if state_errors else None,
    }
    object_patch["status"]["koreo"] = koreo_value

    await patch(object_patch)

    return Ok(time.monotonic())
