from typing import Iterable, Sequence
from unittest.mock import AsyncMock
import asyncio
import random
import time
import unittest

import kr8s.asyncio

from controller import events
from controller import kind_lookup
from controller import scheduler


def _lookup_kind(full_kind: str):
    return (None, f"{full_kind.split('.')[0].lower()}s", None)


class FakeResource:
    def __init__(self, name: str, full_kind: str):
        self.name = name
        self.full_kind = full_kind


def _simple_async_watcher(watch_source: dict[str, Sequence[FakeResource]]):
    async def async_watch(kind: str, namespace: str):
        for result in watch_source.get(kind, []):
            yield "ADDED", result

        # Simulate the watch waiting for updates
        await asyncio.sleep(10)

    return async_watch


def _fake_resource_mapper(
    kinds_and_counts: Iterable[tuple[str, str, int]],
) -> dict[str, Sequence[FakeResource]]:
    return {
        f"{kind.lower()}s.{api_version}": [
            FakeResource(f"resource-{r}", f"{kind.lower()}s.{api_version}")
            for r in range(count)
        ]
        for api_version, kind, count in kinds_and_counts
    }


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        kind_lookup._reset()

    async def test_events_and_scheduler(self):
        mock_api = AsyncMock(kr8s.asyncio.Api)

        mock_api.lookup_kind.side_effect = _lookup_kind

        kind_watch_results = _fake_resource_mapper(
            [
                ("unit.test/v1test1", "One", random.randint(5, 25)),
                ("unit.test/v1test1", "Two", random.randint(5, 25)),
            ]
        )

        mock_api.async_watch.side_effect = _simple_async_watcher(kind_watch_results)

        namespace = "unit-test"

        request_queue = scheduler.RequestQueue[tuple[str, str]]()

        async def event_handler(event: str, resource: kr8s._objects.APIObject, **_):
            await request_queue.put(
                scheduler.Request(
                    at=time.monotonic(), payload=(resource.full_kind, resource.name)
                )
            )
            return True

        event_config = events.Configuration(
            event_handler=event_handler,
            namespace=namespace,
        )

        processed_counts = {}

        async def work_processor(
            payload: tuple[str, str], sys_error_retries: int, user_retries: int
        ):
            key = ":".join(payload)
            processed_counts[key] = processed_counts.get(key, 0) + 1
            return True

        scheduler_config = scheduler.Configuration(work_processor=work_processor)

        watch_queue = events.WatchQueue()

        for kind in ["One", "Two"]:
            await watch_queue.put(
                events.WatchRequest(
                    api_group="unit.test",
                    api_version="v1test1",
                    kind=kind,
                    workflow="unit-test",
                )
            )

        async with asyncio.timeout(0.1), asyncio.TaskGroup() as tg:
            tg.create_task(
                events.chief_of_the_watch(
                    api=mock_api,
                    tg=tg,
                    watch_requests=watch_queue,
                    configuration=event_config,
                ),
                name="chief-of-the-watch",
            )

            tg.create_task(
                scheduler.orchestrator(
                    tg=tg, requests=request_queue, configuration=scheduler_config
                ),
                name="reconcile-scheduler",
            )

            # Start the watches.
            await asyncio.wait_for(watch_queue.join(), timeout=0.1)

            # Let watch iterators run.
            await asyncio.sleep(0)

            # Ensure requests are processed.
            await asyncio.wait_for(request_queue.join(), timeout=0.1)

            # Stop watch sub-system.
            await watch_queue.put(events.StopTheWatch())
            await asyncio.wait_for(watch_queue.join(), timeout=0.1)

            # Stop request management sub-system.
            await request_queue.put(scheduler.Shutdown())
            await asyncio.wait_for(request_queue.join(), timeout=0.1)

        for item, count in processed_counts.items():
            self.assertEqual(1, count, f"Wrong handle count ({count}) for `{item}`")
