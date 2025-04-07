from unittest.mock import AsyncMock, patch
import asyncio
import random
import time
import unittest

from koreo.result import Retry, PermFail
from controller import scheduler


class TestWorker(unittest.IsolatedAsyncioTestCase):
    async def test_clean_reconcile(self):
        work = asyncio.Queue()
        requests = asyncio.Queue()

        payload = {
            "api_group": "unit.test",
            "version": "v1test1",
            "plural_kind": "UnitTest",
            "resource_name": "unit-test",
        }
        request = scheduler.Request(at=0, payload=payload)

        await work.put(request)

        frequency_seconds = random.randint(350, 2500)

        values = ({}, frequency_seconds, None, None)

        async def work_processor(
            payload, ok_frequency_seconds, sys_error_retries, user_retries
        ):
            nonlocal values
            values = (payload, ok_frequency_seconds, sys_error_retries, user_retries)
            return values

        configuration = scheduler.Configuration(
            work_processor=work_processor, frequency_seconds=frequency_seconds
        )

        worker_task = asyncio.create_task(
            scheduler._worker_task(
                work=work, requests=requests, configuration=configuration
            )
        )

        await asyncio.wait_for(work.join(), timeout=1)

        self.assertDictEqual(payload, values[0])
        self.assertEqual(frequency_seconds, values[1])
        self.assertEqual(0, values[2])
        self.assertEqual(0, values[3])

        work.put_nowait(scheduler.Shutdown())
        await asyncio.wait_for(work.join(), timeout=1)
        self.assertTrue(worker_task.done())

    async def test_user_retry_retries(self):
        retry_delay = 30

        work_processor_called = False

        async def work_processor(
            payload, ok_frequency_seconds, sys_error_retries, user_retries
        ):
            nonlocal work_processor_called
            work_processor_called = True
            return Retry(message="User Retry", delay=retry_delay)

        configuration = scheduler.Configuration(work_processor=work_processor)

        expect_time_after = time.monotonic() + retry_delay

        work = asyncio.Queue()
        requests = asyncio.Queue()

        await work.put(
            scheduler.Request(
                at=0,
                payload={},
            )
        )

        work_task = asyncio.create_task(
            scheduler._worker_task(
                work=work, requests=requests, configuration=configuration
            )
        )

        result = await asyncio.wait_for(requests.get(), timeout=1)

        self.assertTrue(requests.empty())

        self.assertTrue(work_processor_called)

        self.assertGreaterEqual(result.at, expect_time_after)
        self.assertLessEqual(result.at, time.monotonic() + retry_delay)

        work.put_nowait(scheduler.Shutdown())
        await asyncio.wait_for(work.join(), timeout=1)
        self.assertTrue(work_task.done())

    async def test_user_retry_retries_multiple_times(self):
        retries = 20

        retry_delay = 10

        work_processor_called = False

        async def work_processor(
            payload, ok_frequency_seconds, sys_error_retries, user_retries
        ):
            nonlocal work_processor_called
            work_processor_called = True
            return Retry(message="User Retry", delay=retry_delay)

        configuration = scheduler.Configuration(work_processor=work_processor)

        work = asyncio.Queue()
        requests = asyncio.Queue()

        await work.put(scheduler.Request(at=0, payload={}))

        work_task = asyncio.create_task(
            scheduler._worker_task(
                work=work, requests=requests, configuration=configuration
            )
        )

        expect_time_after = None
        for _ in range(20):
            expect_time_after = time.monotonic() + retry_delay
            result = await asyncio.wait_for(requests.get(), timeout=1)
            await work.put(result)

        result = await asyncio.wait_for(requests.get(), timeout=1)
        self.assertTrue(requests.empty())

        self.assertGreaterEqual(result.at, expect_time_after)
        self.assertLessEqual(result.at, time.monotonic() + retry_delay)

        self.assertEqual(result.user_retries, retries + 1)

        self.assertTrue(work_processor_called)

        work.put_nowait(scheduler.Shutdown())
        await asyncio.wait_for(work.join(), timeout=1)
        self.assertTrue(work_task.done())

    async def test_user_perm_fail_does_not_retry(self):
        work_processor_called = False

        async def work_processor(
            payload, ok_frequency_seconds, sys_error_retries, user_retries
        ):
            nonlocal work_processor_called
            work_processor_called = True
            return PermFail(message="User PermFail")

        configuration = scheduler.Configuration(work_processor=work_processor)

        work = asyncio.Queue()
        requests = asyncio.Queue()

        await work.put(scheduler.Request(at=0, payload={}))

        work_task = asyncio.create_task(
            scheduler._worker_task(
                work=work, requests=requests, configuration=configuration
            )
        )

        await asyncio.wait_for(work.join(), timeout=1)

        self.assertTrue(requests.empty())

        self.assertTrue(work_processor_called)

        work.put_nowait(scheduler.Shutdown())
        await asyncio.wait_for(work.join(), timeout=1)
        self.assertTrue(work_task.done())

    async def test_reconcile_exception_retries(self):
        work_processor_called = False

        async def work_processor(
            payload, ok_frequency_seconds, sys_error_retries, user_retries
        ):
            nonlocal work_processor_called
            work_processor_called = True
            raise Exception("unit-test-reconcile-error")

        retry_delay_base = random.randint(30, 90)
        retry_delay_jitter = random.randint(10, 30)
        configuration = scheduler.Configuration(
            work_processor=work_processor,
            retry_delay_base=retry_delay_base,
            retry_delay_jitter=retry_delay_jitter,
        )

        expect_time_after = time.monotonic() + retry_delay_base

        work = asyncio.Queue()
        requests = asyncio.Queue()

        await work.put(scheduler.Request(at=0, payload={}))

        work_task = asyncio.create_task(
            scheduler._worker_task(
                work=work, requests=requests, configuration=configuration
            )
        )

        result = await asyncio.wait_for(requests.get(), timeout=1)

        self.assertTrue(requests.empty())
        self.assertTrue(work_processor_called)

        self.assertEqual(result.sys_error_retries, 1)
        self.assertGreaterEqual(result.at, expect_time_after)
        self.assertLessEqual(
            result.at, time.monotonic() + retry_delay_base + retry_delay_jitter
        )

        work.put_nowait(scheduler.Shutdown())
        await asyncio.wait_for(work.join(), timeout=1)
        self.assertTrue(work_task.done())

    async def test_reconcile_exception_retries_exponential_backoff(self):
        work_processor_call_count = 0

        async def work_processor(
            payload, ok_frequency_seconds, sys_error_retries, user_retries
        ):
            nonlocal work_processor_call_count
            work_processor_call_count += 1
            raise Exception("unit-test-reconcile-error")

        retry_max_retries = random.randint(5, 25)
        retry_delay_base = random.randint(30, 90)
        retry_delay_jitter = random.randint(10, 30)
        retry_delay_max = random.randint(
            round((2**retry_max_retries * retry_delay_base) * 2 / 3),
            (2**retry_max_retries * retry_delay_base),
        )
        configuration = scheduler.Configuration(
            work_processor=work_processor,
            retry_max_retries=retry_max_retries,
            retry_delay_base=retry_delay_base,
            retry_delay_jitter=retry_delay_jitter,
            retry_delay_max=retry_delay_max,
        )

        expect_time_after = time.monotonic() + retry_delay_base

        work = asyncio.Queue()
        requests = asyncio.Queue()

        request = scheduler.Request(at=0, payload={})

        # retry-at must be after the time it was inserted for processing
        insert_time = time.monotonic()
        await work.put(request)

        work_task = asyncio.create_task(
            scheduler._worker_task(
                work=work, requests=requests, configuration=configuration
            )
        )

        for retry_count in range(0, retry_max_retries - 1):
            result = await asyncio.wait_for(requests.get(), timeout=1)

            self.assertEqual(result.sys_error_retries, retry_count + 1)

            backoff = min((2**retry_count) * retry_delay_base, retry_delay_max)
            expect_time_after = insert_time + backoff

            self.assertGreaterEqual(result.at, expect_time_after)
            self.assertLessEqual(
                result.at,
                time.monotonic() + backoff + retry_delay_jitter,
            )

            insert_time = time.monotonic()
            await work.put(result)

        await asyncio.sleep(0)

        # result = await asyncio.wait_for(reconciliation_requests.get(), timeout=1)
        self.assertTrue(requests.empty())

        self.assertEqual(retry_max_retries, work_processor_call_count)

        work.put_nowait(scheduler.Shutdown())
        await asyncio.wait_for(work.join(), timeout=1)
        self.assertTrue(work_task.done())


class TestOrchestrator(unittest.IsolatedAsyncioTestCase):
    @patch("controller.scheduler._worker_task")
    async def test_workers_are_spawned(self, worker_task_mock):
        requests = AsyncMock(asyncio.PriorityQueue)
        requests.get.side_effect = asyncio.CancelledError()

        work_processor_called = False

        async def work_processor(
            payload, ok_frequency_seconds, sys_error_retries, user_retries
        ):
            nonlocal work_processor_called
            work_processor_called = True
            return True

        test_worker_count = random.randint(2, 20)
        configuration = scheduler.Configuration(
            work_processor=work_processor, concurrency=test_worker_count
        )

        async with asyncio.timeout(0.1), asyncio.TaskGroup() as tg:
            orchestrator_task = tg.create_task(
                scheduler.orchestrator(tg, requests, configuration)
            )

            requests.put_nowait(scheduler.Shutdown())

        self.assertFalse(work_processor_called)
        self.assertEqual(test_worker_count, worker_task_mock.call_count)
        self.assertTrue(requests.get.called)

    async def test_ready_incoming_are_passed_through(self):
        work_processor_call_count = 0

        work_processor_called_values = {}

        async def work_processor(
            payload, ok_frequency_seconds, sys_error_retries, user_retries
        ):
            nonlocal work_processor_call_count, work_processor_called_values
            work_processor_call_count += 1
            work_processor_called_values = {
                "payload": payload,
                "ok_frequency_seconds": ok_frequency_seconds,
                "sys_error_retries": sys_error_retries,
                "user_retries": user_retries,
            }
            return True

        configuration = scheduler.Configuration(
            work_processor=work_processor, concurrency=1
        )

        requests = AsyncMock(asyncio.PriorityQueue)

        payload = {
            "api_group": "unit.test",
            "version": "v1test1",
            "plural_kind": "UnitTest",
            "resource_name": "unit-test",
        }
        scheduled = scheduler.Request(at=time.monotonic(), payload=payload)

        requests.get.side_effect = [scheduled, asyncio.CancelledError()]

        try:
            async with asyncio.timeout(0.1), asyncio.TaskGroup() as tg:
                tg.create_task(scheduler.orchestrator(tg, requests, configuration))
                await asyncio.sleep(0)
                requests.put_nowait(scheduler.Shutdown())
        except asyncio.TimeoutError:
            pass

        self.assertEqual(2, requests.get.call_count)
        self.assertEqual(1, work_processor_call_count)
        self.assertDictEqual(payload, work_processor_called_values.get("payload", {}))

    @patch("asyncio.wait_for")
    async def test_not_ready_incoming_are_held(self, asyncio_wait_for_patch):
        work_processor_called = False

        async def work_processor(
            payload, ok_frequency_seconds, sys_error_retries, user_retries
        ):
            nonlocal work_processor_called
            work_processor_called = True
            return True

        configuration = scheduler.Configuration(
            work_processor=work_processor, concurrency=1
        )

        requests = AsyncMock(asyncio.PriorityQueue)
        requests.get.return_value = None

        delay_amount = random.randint(30, 3000)
        insert_time = time.monotonic()

        delayed_work = scheduler.Request(at=insert_time + delay_amount, payload={})

        asyncio_wait_for_patch.side_effect = [
            delayed_work,
            asyncio.CancelledError(),
            asyncio.CancelledError(),
        ]

        try:
            async with asyncio.timeout(0), asyncio.TaskGroup() as tg:
                tg.create_task(scheduler.orchestrator(tg, requests, configuration))
                await asyncio.sleep(0)
        except asyncio.TimeoutError:
            pass

        self.assertFalse(work_processor_called)

        self.assertEqual(asyncio_wait_for_patch.call_count, 2)

        last_call = asyncio_wait_for_patch.call_args.kwargs
        timeout = last_call.get("timeout", 0)
        self.assertAlmostEqual(delay_amount, round(timeout))

        # Cleanup
        for call in asyncio_wait_for_patch.call_args_list:
            try:
                get_call = asyncio.create_task(call[0][0])
                get_call.cancel()
                await get_call
            except asyncio.CancelledError:
                pass

    @patch("asyncio.wait_for")
    async def test_wait_timeout(self, asyncio_wait_for_patch):
        work_processor_called = False

        async def work_processor(
            payload, ok_frequency_seconds, sys_error_retries, user_retries
        ):
            nonlocal work_processor_called
            work_processor_called = True
            return True

        configuration = scheduler.Configuration(
            work_processor=work_processor, concurrency=1
        )

        requests = AsyncMock(asyncio.PriorityQueue)
        requests.get.return_value = None

        asyncio_wait_for_patch.side_effect = [
            asyncio.TimeoutError(),
            asyncio.TimeoutError(),
            asyncio.CancelledError(),
        ]

        try:
            async with asyncio.timeout(0), asyncio.TaskGroup() as tg:
                tg.create_task(scheduler.orchestrator(tg, requests, configuration))
                await asyncio.sleep(0)
        except asyncio.TimeoutError:
            pass

        self.assertFalse(work_processor_called)

        self.assertEqual(asyncio_wait_for_patch.call_count, 3)

        last_call = asyncio_wait_for_patch.call_args.kwargs
        timeout = last_call.get("timeout", 0)
        self.assertAlmostEqual(120, round(timeout))

        # Cleanup
        for call in asyncio_wait_for_patch.call_args_list:
            try:
                get_call = asyncio.create_task(call[0][0])
                get_call.cancel()
                await get_call
            except asyncio.CancelledError:
                pass
