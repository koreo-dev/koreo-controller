import kr8s

from koreo import schema

GROUP = "koreo.dev"
VERSION = "v1beta1"
API_VERSION = f"{GROUP}/{VERSION}"

KINDS = [
    ("FunctionTest", "functiontests"),
    ("ResourceFunction", "resourcefunctions"),
    ("ResourceTemplate", "resourcetemplates"),
    ("ValueFunction", "valuefunctions"),
    ("Workflow", "workflows"),
]


def load_koreo_resource_schemas():
    for kind, plural in KINDS:
        crd = kr8s.objects.CustomResourceDefinition.get(name=f"{plural}.{GROUP}")
        schema.load_validator(kind, crd.raw)
