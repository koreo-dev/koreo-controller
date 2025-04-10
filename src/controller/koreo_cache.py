import asyncio
import logging
import random

import kr8s.asyncio

from koreo.cache import delete_resource_from_cache, prepare_and_cache

logger = logging.getLogger("koreo.controller.koreo_cache")

MAX_SYS_ERRORS = 10

RETRY_MAX_DELAY = 300
RETRY_BASE_DELAY = 10
RETRY_JITTER = 5


async def load_cache(
    api: kr8s.asyncio.Api,
    namespace: str,
    api_version: str,
    plural_kind: str,
    kind_title: str,
    resource_class,
    preparer,
):
    logger.debug(f"Building initial {plural_kind}.{api_version} cache.")

    k8s_resource_class = kr8s.objects.new_class(
        version=api_version,
        kind=kind_title,
        plural=plural_kind,
        namespaced=True,
        scalable=False,
        asyncio=True,
    )
    resources = k8s_resource_class.list(api=api, namespace=namespace)

    async for resource in resources:
        logger.debug(f"Caching {resource.name}.")
        await prepare_and_cache(
            resource_class=resource_class,
            preparer=preparer,
            metadata=resource.metadata,
            spec=resource.raw.get("spec", {}),
        )

    logger.debug(f"Initial {plural_kind}.{api_version} cache load complete.")


async def maintain_cache(
    api: kr8s.asyncio.Api,
    namespace: str,
    api_version: str,
    plural_kind: str,
    kind_title: str,
    resource_class,
    preparer,
    reconnect_timeout: int = 900,
):
    logger.debug(f"Maintaining {plural_kind}.{api_version} Cache.")

    k8s_resource_class = kr8s.objects.new_class(
        version=api_version,
        kind=kind_title,
        plural=plural_kind,
        namespaced=True,
        scalable=False,
        asyncio=True,
    )

    error_retries = 0

    while True:
        try:
            watcher = api.async_watch(kind=k8s_resource_class, namespace=namespace)

            async with asyncio.timeout(reconnect_timeout):
                async for event, resource in watcher:
                    if event == "DELETED":
                        logger.debug(
                            f"Deleting {plural_kind}.{api_version} from cache due to {event} for {resource.name}."
                        )
                        await delete_resource_from_cache(
                            resource_class=resource_class,
                            metadata=resource.metadata,
                        )
                        continue

                    logger.debug(
                        f"Updating {plural_kind}.{api_version} cache due to {event} for {resource.name}."
                    )
                    await prepare_and_cache(
                        resource_class=resource_class,
                        preparer=preparer,
                        metadata=resource.metadata,
                        spec=resource.raw.get("spec"),
                    )
        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except asyncio.TimeoutError:
            logger.debug(
                f"Restarting {plural_kind}.{api_version} cache maintainer watch "
                "due to normal reconnect timeout."
            )
            error_retries = 0
        except:
            logger.exception(
                f"Restarting {plural_kind}.{api_version} cache maintainer watch."
            )

            error_retries += 1

            if error_retries > MAX_SYS_ERRORS:
                logger.error(f"Retry limit ({MAX_SYS_ERRORS}) exceeded.")
                raise

            # NOTE: This is just to prevent completely blowing up the API
            # Server if there's an issue. It probably should have a back-off
            # based on the last retry time.
            await asyncio.sleep(
                min((2**error_retries) * RETRY_BASE_DELAY, RETRY_MAX_DELAY)
                + random.random() * RETRY_JITTER
            )
