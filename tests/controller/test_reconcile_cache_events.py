from unittest.mock import AsyncMock
import random
import string
import time
import unittest

import kr8s

from controller.reconcile import get_event_handler


def _get_name():
    return "".join(
        random.choices(population=string.ascii_lowercase, k=random.randint(5, 25))
    )


class TestCacher(unittest.IsolatedAsyncioTestCase):
    async def test_cache_a_new_resource(self):
        handler, update_queue = get_event_handler(namespace="unit-test")

        self.assertEqual(0, update_queue.qsize())

        fake_resource = AsyncMock(kr8s._objects.APIObject)

        resource_name = _get_name()
        resource_uid = _get_name()
        fake_resource.name = resource_name

        fake_resource.raw = {
            "apiVersion": "unit.test/v1test1",
            "kind": "UnitTest",
            "metadata": {
                "name": resource_name,
                "namespace": "unit-test",
                "uid": resource_uid,
            },
            "spec": {"value": _get_name()},
        }

        floor_time = time.monotonic()

        await handler(
            event="ADDED",
            api_group="unit.test",
            api_version="v1test1",
            kind="UnitTest",
            resource=fake_resource,
        )

        update_notification = await update_queue.get()

        self.assertLessEqual(floor_time, update_notification.at)
        self.assertEqual(0, update_notification.user_retries)
        self.assertEqual(0, update_notification.sys_error_retries)

        payload = update_notification.payload
        self.assertEqual(payload.group, "unit.test")
        self.assertEqual(payload.version, "v1test1")
        self.assertEqual(payload.kind, "UnitTest")
        self.assertEqual(payload.name, resource_name)
        self.assertEqual(payload.namespace, "unit-test")

    async def test_same_updates_pass_through(self):
        handler, update_queue = get_event_handler(namespace="unit-test")

        self.assertEqual(0, update_queue.qsize())

        fake_resource = AsyncMock(kr8s._objects.APIObject)

        resource_name = _get_name()
        resource_uid = _get_name()
        fake_resource.name = resource_name

        fake_resource.raw = {
            "apiVersion": "unit.test/v1test1",
            "kind": "UnitTest",
            "metadata": {
                "name": resource_name,
                "namespace": "unit-test",
                "uid": resource_uid,
            },
        }

        test_times = []

        for _ in range(10):
            test_times.append(time.monotonic())

            await handler(
                event="ADDED",
                api_group="unit.test",
                api_version="v1test1",
                kind="UnitTest",
                resource=fake_resource,
            )

        for min_time in test_times:
            update_notification = await update_queue.get()

            self.assertLessEqual(min_time, update_notification.at)
            self.assertEqual(0, update_notification.user_retries)
            self.assertEqual(0, update_notification.sys_error_retries)

            payload = update_notification.payload
            self.assertEqual(payload.group, "unit.test")
            self.assertEqual(payload.version, "v1test1")
            self.assertEqual(payload.kind, "UnitTest")
            self.assertEqual(payload.name, resource_name)
            self.assertEqual(payload.namespace, "unit-test")

    async def test_delete(self):
        handler, update_queue = get_event_handler(namespace="unit-test")

        self.assertEqual(0, update_queue.qsize())

        fake_resource = AsyncMock(kr8s._objects.APIObject)

        resource_name = _get_name()
        resource_uid = _get_name()
        fake_resource.name = resource_name

        fake_resource.raw = {
            "apiVersion": "unit.test/v1test1",
            "kind": "UnitTest",
            "metadata": {
                "name": resource_name,
                "namespace": "unit-test",
                "uid": resource_uid,
            },
        }

        await handler(
            event="DELETED",
            api_group="unit.test",
            api_version="v1test1",
            kind="UnitTest",
            resource=fake_resource,
        )

        self.assertEqual(0, update_queue.qsize())

    async def test_post_delete_updates_ignored(self):
        handler, update_queue = get_event_handler(namespace="unit-test")

        self.assertEqual(0, update_queue.qsize())

        fake_resource = AsyncMock(kr8s._objects.APIObject)

        resource_name = _get_name()
        resource_uid = _get_name()
        fake_resource.name = resource_name

        fake_resource.raw = {
            "apiVersion": "unit.test/v1test1",
            "kind": "UnitTest",
            "metadata": {
                "name": resource_name,
                "namespace": "unit-test",
                "uid": resource_uid,
            },
        }

        await handler(
            event="DELETED",
            api_group="unit.test",
            api_version="v1test1",
            kind="UnitTest",
            resource=fake_resource,
        )

        await handler(
            event="ADDED",
            api_group="unit.test",
            api_version="v1test1",
            kind="UnitTest",
            resource=fake_resource,
        )

        self.assertEqual(0, update_queue.qsize())

    async def test_readding_deleted_notifies(self):
        handler, update_queue = get_event_handler(namespace="unit-test")

        self.assertEqual(0, update_queue.qsize())

        fake_resource = AsyncMock(kr8s._objects.APIObject)

        resource_name = _get_name()
        fake_resource.name = resource_name

        fake_resource.raw = {
            "apiVersion": "unit.test/v1test1",
            "kind": "UnitTest",
            "metadata": {
                "name": resource_name,
                "namespace": "unit-test",
                "uid": _get_name(),
            },
        }

        await handler(
            event="DELETED",
            api_group="unit.test",
            api_version="v1test1",
            kind="UnitTest",
            resource=fake_resource,
        )

        fake_resource.raw["metadata"]["uid"] = _get_name()

        floor_time = time.monotonic()
        await handler(
            event="ADDED",
            api_group="unit.test",
            api_version="v1test1",
            kind="UnitTest",
            resource=fake_resource,
        )

        update_notification = await update_queue.get()

        self.assertLessEqual(floor_time, update_notification.at)
        self.assertEqual(0, update_notification.user_retries)
        self.assertEqual(0, update_notification.sys_error_retries)

        payload = update_notification.payload
        self.assertEqual(payload.group, "unit.test")
        self.assertEqual(payload.version, "v1test1")
        self.assertEqual(payload.kind, "UnitTest")
        self.assertEqual(payload.name, resource_name)
        self.assertEqual(payload.namespace, "unit-test")
