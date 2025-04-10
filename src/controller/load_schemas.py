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


async def load_koreo_resource_schemas(api: kr8s.asyncio.Api):
    async with asyncio.TaskGroup() as tg:
        for kind, plural in KINDS:
            tg.create_task(
                _load_schema_for_kind(api, kind, plural), name=f"schema-loader-{kind}"
            )


async def _load_schema_for_kind(api: kr8s.asyncio.Api, kind: str, plural: str):
    crd = await kr8s.asyncio.objects.CustomResourceDefinition.get(
        api=api, name=f"{plural}.{API_GROUP}"
    )
    schema.load_validator(kind, crd.raw)
