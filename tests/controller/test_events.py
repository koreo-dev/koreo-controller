from typing import Iterable, Sequence
from unittest.mock import AsyncMock
import asyncio
import random
import unittest

import kr8s.asyncio

from controller import kind_lookup
from controller import events


class FakeResource:
    def __init__(self, name: str, full_kind: str):
        self.name = name
        self.full_kind = full_kind


def _lookup_kind(full_kind: str):
    return (None, f"{full_kind.split('.')[0].lower()}s", None)


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


class TestChiefOfTheWatch(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        kind_lookup._reset()

    async def test_setup(self):
        """
        Start watches on several kinds, ensure the handler is called for each
        resource.
        """

        mock_api = AsyncMock(kr8s.asyncio.Api)

        mock_api.lookup_kind.side_effect = _lookup_kind

        test_watches = random.randint(2, 10)

        kind_watch_results = _fake_resource_mapper(
            ("unit.test/v1test1", f"UnitTest{i}", random.randint(2, 10))
            for i in range(test_watches)
        )

        mock_api.async_watch.side_effect = _simple_async_watcher(kind_watch_results)

        kind_events_handeled = {
            f"{kind}:{resource.name}": 0
            for kind in kind_watch_results.keys()
            for resource in kind_watch_results[kind]
        }

        async def event_handler(event: str, resource):
            kind = resource.full_kind
            name = resource.name
            kind_events_handeled[f"{kind}:{name}"] += 1
            print(
                f"{event} for {kind}:{name} ({kind_events_handeled[f'{kind}:{name}']})"
            )
            return True

        watch_requests = asyncio.Queue()

        for i in range(test_watches):
            await watch_requests.put(
                events.WatchRequest(
                    api_group="unit.test",
                    api_version="v1test1",
                    kind=f"UnitTest{i}",
                    workflow=f"unit-test-workflow-{i}",
                )
            )

        configuration = events.Configuration(
            event_handler=event_handler,
            namespace="unit-test",
        )

        async with asyncio.timeout(0.1), asyncio.TaskGroup() as tg:
            chief_task = tg.create_task(
                events.chief_of_the_watch(
                    api=mock_api,
                    tg=tg,
                    watch_requests=watch_requests,
                    configuration=configuration,
                ),
                name="chief-of-the-watch-unit-test",
            )

            await asyncio.wait_for(watch_requests.join(), timeout=0.1)
            self.assertEqual(0, watch_requests.qsize())

            await asyncio.sleep(0)
            await watch_requests.put(events.StopTheWatch())

        for resource, count in kind_events_handeled.items():
            self.assertEqual(1, count, msg=f"Too many events {count} for {resource}.")

        self.assertTrue(chief_task.done())
        self.assertIsNone(chief_task.result())

    async def test_multiple_watchers(self):
        """
        Request the same watch several times and ensure the watch restarts.
        (idempotency)
        """

        mock_api = AsyncMock(kr8s.asyncio.Api)

        mock_api.lookup_kind.side_effect = _lookup_kind

        resource_queue: asyncio.Queue[FakeResource] = asyncio.Queue()
        kind_watch_queues = {"unittests.unit.test/v1test1": resource_queue}

        watch_call_count = 0

        async def async_watch(kind: str, namespace: str):
            nonlocal watch_call_count
            watch_call_count += 1

            queue = kind_watch_queues.get(kind)
            if not queue:
                return

            while True:
                yield "ADDED", await queue.get()

        mock_api.async_watch.side_effect = async_watch

        handled_event_count = 0

        async def event_handler(event: str, resource):
            nonlocal handled_event_count
            handled_event_count += 1
            return True

        watch_requests = asyncio.Queue()

        test_workflows = [
            f"unit-test-workflow-{workflow_id}"
            for workflow_id in range(random.randint(3, 15))
        ]

        for workflow_id in test_workflows:
            await watch_requests.put(
                events.WatchRequest(
                    api_group="unit.test",
                    api_version="v1test1",
                    kind=f"UnitTest",
                    workflow=workflow_id,
                )
            )

        # Test removing some watchers
        for workflow_id in test_workflows[1:]:
            await watch_requests.put(events.CancelWatches(workflow=workflow_id))

        configuration = events.Configuration(
            event_handler=event_handler,
            namespace="unit-test",
        )

        test_resource_events = random.randint(3, 15)

        async with asyncio.timeout(0.1), asyncio.TaskGroup() as tg:
            chief_task = tg.create_task(
                events.chief_of_the_watch(
                    api=mock_api,
                    tg=tg,
                    watch_requests=watch_requests,
                    configuration=configuration,
                ),
                name="chief-of-the-watch-unit-test",
            )

            await asyncio.wait_for(watch_requests.join(), timeout=0.1)
            self.assertEqual(0, watch_requests.qsize())

            for r in range(test_resource_events):
                await resource_queue.put(
                    FakeResource(
                        name=f"resource-{r}", full_kind="unittests.unit.test/v1test1"
                    )
                )

            await asyncio.sleep(0)
            await watch_requests.put(events.StopTheWatch())

        # Watch (re)starts once per watch, and once for each removal. With one
        # remaining.
        self.assertEqual(len(test_workflows) * 2 - 1, watch_call_count)
        self.assertEqual(test_resource_events, handled_event_count)

        self.assertTrue(chief_task.done())
        self.assertIsNone(chief_task.result())

    async def test_update_watched_resource(self):
        mock_api = AsyncMock(kr8s.asyncio.Api)

        mock_api.lookup_kind.side_effect = _lookup_kind

        kind_watch_results = _fake_resource_mapper(
            [
                ("unit.test/v1test1", f"Original", random.randint(2, 10)),
                ("unit.test/v1test1", f"Update", random.randint(2, 10)),
            ]
        )

        async def async_watch(kind: str, namespace: str):
            # This is just to make the test clearer (delay sending resources)
            if kind.startswith("original"):
                await asyncio.sleep(1)

            for result in kind_watch_results.get(kind, []):
                yield "ADDED", result

            # Simulate the watch waiting for updates
            # with self.assertRaises(asyncio.CancelledError):
            await asyncio.sleep(10)

        mock_api.async_watch.side_effect = async_watch

        kind_events_handeled = {
            f"{kind}:{resource.name}": 0
            for kind in kind_watch_results.keys()
            for resource in kind_watch_results[kind]
        }

        async def event_handler(event: str, resource):
            kind = resource.full_kind
            name = resource.name
            print(f"handling and event for {kind}:{name}")

            kind_events_handeled[f"{kind}:{name}"] += 1
            print(
                f"{event} for {kind}:{name} ({kind_events_handeled[f'{kind}:{name}']})"
            )
            return True

        configuration = events.Configuration(
            event_handler=event_handler,
            namespace="unit-test",
        )

        watch_requests = asyncio.Queue()

        async with asyncio.timeout(0.1), asyncio.TaskGroup() as tg:
            chief_task = tg.create_task(
                events.chief_of_the_watch(
                    api=mock_api,
                    tg=tg,
                    watch_requests=watch_requests,
                    configuration=configuration,
                ),
                name="chief-of-the-watch-unit-test",
            )

            # Start the "wrong" watch.
            await watch_requests.put(
                events.WatchRequest(
                    api_group="unit.test",
                    api_version="v1test1",
                    kind="original",
                    workflow=f"unit-test-workflow",
                )
            )
            # Ensure it is processed.
            await asyncio.wait_for(watch_requests.join(), timeout=0.1)

            # Start the "right" watch.
            await watch_requests.put(
                events.WatchRequest(
                    api_group="unit.test",
                    api_version="v1test1",
                    kind="update",
                    workflow=f"unit-test-workflow",
                )
            )
            # Ensure it is processed.
            await asyncio.wait_for(watch_requests.join(), timeout=0.1)

            # All watches ought to be procesed now.
            self.assertEqual(0, watch_requests.qsize())

            await watch_requests.put(events.StopTheWatch())
            await asyncio.wait_for(watch_requests.join(), timeout=0.1)

        print(kind_events_handeled)

        for resource, count in kind_events_handeled.items():
            if resource.startswith("original"):
                expected_count = 0
            else:
                expected_count = 1
            self.assertEqual(
                expected_count, count, msg=f"Wrong event count for {resource}."
            )

        self.assertTrue(chief_task.done())
        self.assertIsNone(chief_task.result())

    async def test_workflow_deletion(self):
        """Simulate the deletion of a workflow."""

        mock_api = AsyncMock(kr8s.asyncio.Api)

        mock_api.lookup_kind.side_effect = _lookup_kind

        resource_queue: asyncio.Queue[FakeResource] = asyncio.Queue()
        kind_watch_queues = {"unittests.unit.test/v1test1": resource_queue}

        watch_call_count = 0

        async def async_watch(kind: str, namespace: str):
            nonlocal watch_call_count
            watch_call_count += 1

            queue = kind_watch_queues.get(kind)
            if not queue:
                return

            while True:
                yield "ADDED", await queue.get()
                raise Exception("This should never fire.")

        mock_api.async_watch.side_effect = async_watch

        handled_event_count = 0

        async def event_handler(event: str, resource):
            nonlocal handled_event_count
            handled_event_count += 1
            return True

        watch_requests = asyncio.Queue()

        await watch_requests.put(
            events.WatchRequest(
                api_group="unit.test",
                api_version="v1test1",
                kind=f"UnitTest",
                workflow=f"unit-test-workflow",
            )
        )

        configuration = events.Configuration(
            event_handler=event_handler,
            namespace="unit-test",
        )

        test_resource_events = random.randint(3, 15)

        async with asyncio.timeout(0.1), asyncio.TaskGroup() as tg:
            chief_task = tg.create_task(
                events.chief_of_the_watch(
                    api=mock_api,
                    tg=tg,
                    watch_requests=watch_requests,
                    configuration=configuration,
                ),
                name="chief-of-the-watch-unit-test",
            )

            await asyncio.wait_for(watch_requests.join(), timeout=0.1)
            self.assertEqual(0, watch_requests.qsize())

            await watch_requests.put(
                events.CancelWatches(
                    workflow=f"unit-test-workflow",
                )
            )

            await asyncio.wait_for(watch_requests.join(), timeout=0.1)
            self.assertEqual(0, watch_requests.qsize())
            await asyncio.sleep(0)

            for r in range(test_resource_events):
                await resource_queue.put(
                    FakeResource(
                        name=f"resource-{r}", full_kind="unittests.unit.test/v1test1"
                    )
                )

            await asyncio.sleep(0)
            await watch_requests.put(events.StopTheWatch())

        self.assertEqual(1, watch_call_count)  # Only called at setup
        self.assertEqual(0, handled_event_count)  # Stopped before any events.

        self.assertTrue(chief_task.done())
        self.assertIsNone(chief_task.result())

    async def test_watch_restarting(self):
        """Watches auto-restart on error within handler."""

        mock_api = AsyncMock(kr8s.asyncio.Api)

        mock_api.lookup_kind.side_effect = _lookup_kind

        watch_call_count = 0

        async def async_watch(kind: str, namespace: str):
            nonlocal watch_call_count
            watch_call_count += 1

            print(f"starting watch {watch_call_count}")

            yield (
                "ADDED",
                FakeResource(name="unit-test", full_kind="unittests.unit.test/v1test1"),
            )

        mock_api.async_watch.side_effect = async_watch

        handled_event_count = 0

        async def event_handler(event: str, resource):
            nonlocal handled_event_count
            handled_event_count += 1
            print(f"Handling event {handled_event_count}")
            raise Exception("unit-test-boom-boom")

        watch_requests = asyncio.Queue()

        await watch_requests.put(
            events.WatchRequest(
                api_group="unit.test",
                api_version="v1test1",
                kind=f"UnitTest",
                workflow="unit-test-workflow",
            )
        )

        max_errors = random.randint(3, 15)

        configuration = events.Configuration(
            event_handler=event_handler,
            namespace="unit-test",
            max_unknown_errors=max_errors,
        )

        async with asyncio.timeout(0.1), asyncio.TaskGroup() as tg:
            chief_task = tg.create_task(
                events.chief_of_the_watch(
                    api=mock_api,
                    tg=tg,
                    watch_requests=watch_requests,
                    configuration=configuration,
                ),
                name="chief-of-the-watch-unit-test",
            )

            await asyncio.wait_for(watch_requests.join(), timeout=0.1)

            await watch_requests.put(events.StopTheWatch())

        self.assertEqual(max_errors, watch_call_count)
        self.assertEqual(max_errors, handled_event_count)

        self.assertTrue(chief_task.done())
        self.assertIsNone(chief_task.result())
