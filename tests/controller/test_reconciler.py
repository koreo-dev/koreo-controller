from unittest.mock import AsyncMock
import random
import string
import unittest

import kr8s

from controller import reconcile


def _get_name():
    return "".join(
        random.choices(population=string.ascii_lowercase, k=random.randint(5, 25))
    )


class TestReconcileResource(unittest.IsolatedAsyncioTestCase):
    async def test_reconcile_uncached_is_a_noop(self):
        mock_api = AsyncMock(kr8s.asyncio.Api)

        resource = reconcile.Resource(
            group="unit.test",
            version="v1test1",
            kind="UnitTest",
            name=_get_name(),
            namespace=_get_name(),
        )
        result = await reconcile.reconcile_resource(
            api=mock_api,
            payload=resource,
            ok_frequency_seconds=60,
            sys_error_retries=0,
            user_retries=0,
        )

        self.assertIsNone(result)

    async def test_reconcile_cached(self):
        mock_api = AsyncMock(kr8s.asyncio.Api)

        cacher, queue = reconcile.get_event_handler(namespace="unit-test")

        resource_name = _get_name()
        resource_uid = _get_name()

        fake_resource = AsyncMock(kr8s._objects.APIObject)
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

        await cacher(
            event="ADDED",
            api_group="unit.test",
            api_version="v1test1",
            kind="UnitTest",
            resource=fake_resource,
        )

        event = await queue.get()
        queue.task_done()

        resource = reconcile.Resource(
            group="unit.test",
            version="v1test1",
            kind="UnitTest",
            name=_get_name(),
            namespace=_get_name(),
        )

        result = await reconcile.reconcile_resource(
            api=mock_api,
            payload=resource,
            ok_frequency_seconds=60,
            sys_error_retries=0,
            user_retries=0,
        )

        self.assertIsNone(result)
