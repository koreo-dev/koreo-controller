import asyncio
import logging
import random

import kr8s.asyncio

logger = logging.getLogger("koreo.controller.resource")

LOOKUP_TIMEOUT = 15

LOOKUP_TIMEOUT_RETRIES = 3
LOOKUP_TIMEOUT_RETRY_BASE = 2
LOOKUP_TIMEOUT_RETRY_JITTER = 5

LOOKUP_MAX_WAIT = LOOKUP_TIMEOUT * LOOKUP_TIMEOUT_RETRIES

_plural_map: dict[str, str] = {}

# Why a Task and an Event?
# https://docs.python.org/3/library/asyncio-task.html#task-cancellation
# There is a somewhat nasty and possibly non-obvious situation. The
# CancelledError is actually _raised from within the coroutine_ a cancelled
# task is awaiting. That means, in order to spawn a background lookup task that
# multiple tasks (which may be cancelled) are awaiting you need to decouple. We
# use an Event to accomplish that.
_lookup_tasks_and_locks: dict[str, tuple[asyncio.Task[str | None], asyncio.Event]] = {}


async def get_full_kind(
    api: kr8s.asyncio.Api, kind: str, api_version: str
) -> str | None:
    lookup_kind = f"{kind}.{api_version}"

    logger.debug(f"Plural Lookup For '{lookup_kind}'")

    if lookup_kind in _plural_map:
        return _plural_map[lookup_kind]

    lookup_task_and_event = _lookup_tasks_and_locks.get(lookup_kind)
    if lookup_task_and_event:
        task, event = lookup_task_and_event
        try:
            await asyncio.wait_for(event.wait(), timeout=LOOKUP_MAX_WAIT)
            return await asyncio.wait_for(task, timeout=1)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout looking up plural kind for {lookup_kind}")
            raise
        except asyncio.CancelledError:
            logger.debug(
                f"Calling task cancelled while looking up plural kind for {lookup_kind}"
            )
            raise
        except:
            logger.exception(f"Unknown error looking up plural kind for {lookup_kind}")

        return None

    lookup_event = asyncio.Event()
    lookup_task = asyncio.create_task(
        _lookup_plural_kind(
            api=api,
            event=lookup_event,
            lookup_kind=lookup_kind,
            api_version=api_version,
        )
    )
    _lookup_tasks_and_locks[lookup_kind] = (lookup_task, lookup_event)
    lookup_task.add_done_callback(
        lambda _: _lookup_tasks_and_locks.__delitem__(lookup_kind)
    )

    try:
        await asyncio.wait_for(lookup_event.wait(), timeout=LOOKUP_MAX_WAIT)
        return await asyncio.wait_for(lookup_task, timeout=1)
    except asyncio.TimeoutError:
        logger.warning(f"Timeout looking up plural kind for {lookup_kind}")
        raise
    except asyncio.CancelledError:
        logger.debug(
            f"Calling task cancelled while looking up plural kind for {lookup_kind}"
        )
        raise

    return None


async def _lookup_plural_kind(
    api: kr8s.asyncio.Api, event: asyncio.Event, lookup_kind: str, api_version: str
):
    try:
        for retry in range(LOOKUP_TIMEOUT_RETRIES):
            try:
                logger.debug(f"Attempting lookup: {lookup_kind}")
                async with asyncio.timeout(LOOKUP_TIMEOUT):
                    try:
                        (_, plural_kind, _) = await api.lookup_kind(lookup_kind)

                        logger.debug(f"Found Plural from API: {plural_kind}")

                        full_kind = f"{plural_kind}.{api_version}"
                        _plural_map[lookup_kind] = full_kind
                        return full_kind

                    except ValueError:
                        logger.exception(
                            f"Failed to find plural kind (`{lookup_kind}`) "
                            "information; can not start controller for this kind!"
                        )
                        return None

            except asyncio.TimeoutError:
                if retry >= LOOKUP_TIMEOUT_RETRIES - 1:
                    logger.error(
                        f"Too many API timeouts lookup up plural for kind ({lookup_kind})."
                    )
                    raise

                delay = max(
                    (2**retry) * LOOKUP_TIMEOUT_RETRY_BASE
                    + random.randint(0, LOOKUP_TIMEOUT_RETRY_JITTER),
                    LOOKUP_TIMEOUT,
                )

                logger.warning(
                    f"API Timeout looking up plural kind ({lookup_kind}), "
                    f"waiting {delay} to retry."
                )
                await asyncio.sleep(delay)

                continue

            except asyncio.CancelledError as err:
                logger.warning(
                    f"Plural kind Lookup cancelled before completion {lookup_kind}"
                )
                raise err

            except:
                logger.exception(f"Plural kind lookup error for {lookup_kind}")
                raise

        logger.warning(
            f"Too many failed attempts to find plural kind for {lookup_kind}."
        )
        return None

    finally:
        event.set()


def _reset():
    """Helper for unit testing; not intended for usage in normal code."""
    _plural_map.clear()

    for task, event in _lookup_tasks_and_locks.values():
        task.cancel()
        event.set()

    _lookup_tasks_and_locks.clear()
