import random
import string
import unittest

from controller import workflow_registry


def _name_generator(prefix: str):
    return f"{prefix}-{''.join(random.choices(string.ascii_lowercase, k=15))}"


class TestRegistry(unittest.TestCase):
    def setUp(self):
        workflow_registry._reset_registry()

    def test_roundtrip(self):
        custom_crd_name = _name_generator("crd")
        workflow_name_one = _name_generator("workflow")
        workflow_name_two = _name_generator("workflow")

        workflow_registry.index_workflow_custom_crd(
            workflow=workflow_name_one, custom_crd=custom_crd_name
        )
        workflow_registry.index_workflow_custom_crd(
            workflow=workflow_name_two, custom_crd=custom_crd_name
        )

        workflows = workflow_registry.get_custom_crd_workflows(
            custom_crd=custom_crd_name
        )
        self.assertIn(workflow_name_one, workflows)
        self.assertIn(workflow_name_two, workflows)

    def test_no_change_usage_reindex(self):
        workflow_name = _name_generator("workflow")

        custom_crd_name = _name_generator("crd")

        workflow_registry.index_workflow_custom_crd(
            workflow=workflow_name, custom_crd=custom_crd_name
        )
        workflow_registry.index_workflow_custom_crd(
            workflow=workflow_name, custom_crd=custom_crd_name
        )

        self.assertEqual(
            [workflow_name], workflow_registry.get_custom_crd_workflows(custom_crd_name)
        )

    def test_change_usage(self):
        workflow_name = _name_generator("workflow")

        custom_crd_name_one = _name_generator("crd")
        custom_crd_name_two = _name_generator("crd")

        workflow_registry.index_workflow_custom_crd(
            workflow=workflow_name, custom_crd=custom_crd_name_one
        )
        workflow_registry.index_workflow_custom_crd(
            workflow=workflow_name, custom_crd=custom_crd_name_two
        )

        self.assertNotIn(
            workflow_name,
            workflow_registry.get_custom_crd_workflows(custom_crd_name_one),
        )
        self.assertIn(
            workflow_name,
            workflow_registry.get_custom_crd_workflows(custom_crd_name_two),
        )

    def test_unindex(self):
        workflow_name = _name_generator("workflow")

        custom_crd_name = _name_generator("crd")

        workflow_registry.index_workflow_custom_crd(
            workflow=workflow_name, custom_crd=custom_crd_name
        )

        self.assertIn(
            workflow_name,
            workflow_registry.get_custom_crd_workflows(custom_crd_name),
        )

        workflow_registry.unindex_workflow_custom_crd(workflow=workflow_name)

        self.assertNotIn(
            workflow_name,
            workflow_registry.get_custom_crd_workflows(custom_crd_name),
        )

    def test_noop_unindex(self):
        workflow_name = _name_generator("workflow")
        workflow_registry.unindex_workflow_custom_crd(workflow=workflow_name)

    def test_unindex_one_of_multiple(self):
        workflow_one_name = _name_generator("workflow-one")
        workflow_two_name = _name_generator("workflow-two")

        custom_crd_name = _name_generator("crd")

        workflow_registry.index_workflow_custom_crd(
            workflow=workflow_one_name, custom_crd=custom_crd_name
        )
        workflow_registry.index_workflow_custom_crd(
            workflow=workflow_two_name, custom_crd=custom_crd_name
        )

        check_workflows = workflow_registry.get_custom_crd_workflows(custom_crd_name)
        self.assertIn(workflow_one_name, check_workflows)
        self.assertIn(workflow_two_name, check_workflows)

        workflow_registry.unindex_workflow_custom_crd(workflow=workflow_one_name)

        check_workflows = workflow_registry.get_custom_crd_workflows(custom_crd_name)
        self.assertNotIn(workflow_one_name, check_workflows)
        self.assertIn(workflow_two_name, check_workflows)
