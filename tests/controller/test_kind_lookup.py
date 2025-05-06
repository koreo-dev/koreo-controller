from unittest.mock import AsyncMock
import asyncio
import unittest

import kr8s.asyncio

from controller import kind_lookup


class TestGetFullKind(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_lookup_timeout = kind_lookup.LOOKUP_TIMEOUT
        self.original_lookup_timeout_retry_base = kind_lookup.LOOKUP_TIMEOUT_RETRY_BASE
        self.original_lookup_timeout_retry_jitter = (
            kind_lookup.LOOKUP_TIMEOUT_RETRY_JITTER
        )

        kind_lookup.LOOKUP_TIMEOUT = 0.01
        kind_lookup.LOOKUP_TIMEOUT_RETRY_BASE = 0.05
        kind_lookup.LOOKUP_TIMEOUT_RETRY_JITTER = 0

    def tearDown(self):
        kind_lookup._reset()

        kind_lookup.LOOKUP_TIMEOUT = self.original_lookup_timeout
        kind_lookup.LOOKUP_TIMEOUT_RETRY_BASE = self.original_lookup_timeout_retry_base
        kind_lookup.LOOKUP_TIMEOUT_RETRY_JITTER = (
            self.original_lookup_timeout_retry_jitter
        )

    async def test_ok_lookup(self):
        plural_kind = "unittesties"

        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.return_value = (None, plural_kind, None)

        api_version = "unit.test/v1"

        result = await kind_lookup.get_full_kind(api_mock, "UnitTest", api_version)

        self.assertEqual(result, f"{plural_kind}.{api_version}")
        self.assertEqual(1, api_mock.lookup_kind.call_count)

    async def test_successive_lookups(self):
        plural_kind = "unittesties"

        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.return_value = (None, plural_kind, None)

        api_version = "unit.test/v1"

        result = await kind_lookup.get_full_kind(api_mock, "UnitTest", api_version)
        self.assertEqual(result, f"{plural_kind}.{api_version}")

        result = await kind_lookup.get_full_kind(api_mock, "UnitTest", api_version)
        self.assertEqual(result, f"{plural_kind}.{api_version}")

        self.assertEqual(1, api_mock.lookup_kind.call_count)

    async def test_multiple_requests(self):
        plural_kind = "unittesties"

        async def lookup(_):
            await asyncio.sleep(0)
            return (None, plural_kind, None)

        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = lookup

        api_version = "unit.test/v1"

        tasks = [
            asyncio.create_task(
                kind_lookup.get_full_kind(api_mock, "UnitTest", api_version)
            ),
            asyncio.create_task(
                kind_lookup.get_full_kind(api_mock, "UnitTest", api_version)
            ),
            asyncio.create_task(
                kind_lookup.get_full_kind(api_mock, "UnitTest", api_version)
            ),
        ]
        done, pending = await asyncio.wait(tasks)

        self.assertEqual(len(tasks), len(done))
        self.assertEqual(0, len(pending))

        for task in tasks:
            self.assertEqual(task.result(), f"{plural_kind}.{api_version}")

        self.assertEqual(1, api_mock.lookup_kind.call_count)

    async def test_multiple_requests_one_cancelled(self):
        plural_kind = "unittesties"

        async def lookup(_):
            await asyncio.sleep(0)
            return (None, plural_kind, None)

        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = lookup

        api_version = "unit.test/v1"

        async with asyncio.TaskGroup() as tg:
            cancelled_task = tg.create_task(
                kind_lookup.get_full_kind(api_mock, "UnitTest", api_version)
            )
            successful_task = tg.create_task(
                kind_lookup.get_full_kind(api_mock, "UnitTest", api_version)
            )

            cancelled_task.cancel()

        # Done, but cancelled.
        self.assertTrue(cancelled_task.done())
        self.assertTrue(cancelled_task.cancelled())

        # Done, with a value.
        self.assertTrue(successful_task.done())
        self.assertFalse(successful_task.cancelled())
        self.assertEqual(successful_task.result(), f"{plural_kind}.{api_version}")

        self.assertEqual(1, api_mock.lookup_kind.call_count)

    async def test_missing_kind(self):
        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = ValueError("Kind not found")

        api_version = "unit.test/v1"

        full_kind = await kind_lookup.get_full_kind(api_mock, "UnitTest", api_version)

        self.assertIsNone(full_kind)

    async def test_2_timeout_retries(self):
        plural_kind = "unittesties"

        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = [
            asyncio.TimeoutError(),
            (None, plural_kind, None),
        ]

        api_version = "unit.test/v1"

        kind_lookup.LOOKUP_TIMEOUT = 0.1
        kind_lookup.LOOKUP_TIMEOUT_RETRY_BASE = 0.1

        result = await kind_lookup.get_full_kind(api_mock, "UnitTest", api_version)
        self.assertEqual(result, f"{plural_kind}.{api_version}")

    async def test_too_many_timeout_retries(self):
        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = asyncio.TimeoutError()

        api_version = "unit.test/v1"

        kind_lookup.LOOKUP_TIMEOUT = 0.1
        kind_lookup.LOOKUP_TIMEOUT_RETRY_BASE = 0.1

        with self.assertRaises(Exception):
            await kind_lookup.get_full_kind(api_mock, "unittest", api_version)

    async def test_lock_cleared_on_errors(self):
        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = ZeroDivisionError("Unit Test")

        api_version = "unit.test/v1"

        with self.assertRaises(ZeroDivisionError):
            await kind_lookup.get_full_kind(api_mock, "unittest", api_version)
