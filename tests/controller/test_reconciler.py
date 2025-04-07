from unittest.mock import AsyncMock
import random
import string
import time
import unittest

import kr8s

from controller import reconcile


def _get_name():
    return "".join(
        random.choices(population=string.ascii_lowercase, k=random.randint(5, 25))
    )


class TestReconcileResource(unittest.IsolatedAsyncioTestCase):
    async def test_reconcile_uncached_is_a_noop(self):
        resource = reconcile.Resource(
            group="unit.test",
            version="v1test1",
            kind="UnitTest",
            name=_get_name(),
            namespace=_get_name(),
        )
        result = await reconcile.reconcile_resource(
            payload=resource,
            ok_frequency_seconds=60,
            sys_error_retries=0,
            user_retries=0,
        )

        self.assertIsNone(result)
