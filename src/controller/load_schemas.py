import asyncio
import kr8s.asyncio

from koreo import schema
from koreo.constants import API_GROUP, DEFAULT_API_VERSION

API_VERSION = f"{API_GROUP}/{DEFAULT_API_VERSION}"

KINDS = [
    ("FunctionTest", "functiontests"),
    ("ResourceFunction", "resourcefunctions"),
    ("ResourceTemplate", "resourcetemplates"),
    ("ValueFunction", "valuefunctions"),
    ("Workflow", "workflows"),
]


async def load_koreo_resource_schemas():
    tasks = set()

    for kind, plural in KINDS:
        tasks.add(_load_schema_for_kind(kind, plural))

    await asyncio.gather(*tasks)


async def _load_schema_for_kind(kind: str, plural: str):
    crd = await kr8s.asyncio.objects.CustomResourceDefinition.get(
        name=f"{plural}.{API_GROUP}"
    )
    schema.load_validator(kind, crd.raw)
