import copy
import random
import string
import unittest

from koreo.constants import PREFIX
from controller.reconcile import _hash_resource


def _generate_string(min_length, max_length):
    return "".join(
        random.choices(string.ascii_letters, k=random.randint(min_length, max_length))
    )


def _generate_random_dict(
    size=10, max_depth=2, min_value_length=1, max_value_length=10
):
    def _generate_sub_structure(depth: int, type_selector: int = 0):
        if not type_selector:
            type_selector = random.randint(1, 3)

        if type_selector == 1 and depth > 0:
            return {
                _generate_string(3, 15): _generate_sub_structure(depth=depth - 1)
                for _ in range(random.randint(min_value_length, max_value_length))
            }
        elif type_selector == 2 and depth > 0:
            list_value_type = random.randint(1, 3)
            return [
                _generate_sub_structure(depth=depth - 1, type_selector=list_value_type)
                for _ in range(random.randint(min_value_length, max_value_length))
            ]
        else:
            return _generate_string(min_value_length, max_value_length)

    return {
        _generate_string(3, 15): _generate_sub_structure(depth=max_depth)
        for _ in range(size)
    }


def _generate_simple_map(
    min_size=1, max_size=10, min_value_length=1, max_value_length=10
):
    size = random.randint(min_size, max_size)
    if not size:
        return None

    return {
        _generate_string(3, 20): _generate_string(min_value_length, max_value_length)
        for _ in range(size)
    }


class TestResourceHashing(unittest.TestCase):
    def test_simple_hash(self):
        metadata = _generate_random_dict(
            size=10, max_depth=2, min_value_length=5, max_value_length=20
        )

        metadata["labels"] = _generate_simple_map(
            min_size=0, max_size=15, min_value_length=5, max_value_length=20
        )
        metadata["annotations"] = _generate_simple_map(
            min_size=0, max_size=15, min_value_length=5, max_value_length=200
        )

        spec = _generate_random_dict(
            size=10, max_depth=3, min_value_length=5, max_value_length=20
        )

        self.assertEqual(
            _hash_resource({"metadata": metadata, "spec": spec}),
            _hash_resource({"metadata": metadata, "spec": spec}),
        )

    def test_koreo_annotations_ignored(self):
        metadata = _generate_random_dict(
            size=10, max_depth=2, min_value_length=5, max_value_length=20
        )
        metadata2 = copy.copy(metadata)

        annotations = _generate_simple_map(
            min_size=1, max_size=15, min_value_length=5, max_value_length=200
        )
        metadata["annotations"] = copy.copy(annotations)

        for i in range(random.randint(1, 10)):
            annotations[f"{PREFIX}/{_generate_string(3, 5)}{i}"] = _generate_string(
                5, 20
            )
        metadata2["annotations"] = annotations

        spec = _generate_random_dict(
            size=10, max_depth=3, min_value_length=5, max_value_length=20
        )

        self.assertEqual(
            _hash_resource({"metadata": metadata, "spec": spec}),
            _hash_resource({"metadata": metadata2, "spec": spec}),
        )

    def test_order_agnostic(self):
        """Validate hashes match even if structures are reordered."""

        # We want the basic metadata structure to match.
        metadata = _generate_random_dict(
            size=10, max_depth=2, min_value_length=5, max_value_length=20
        )
        metadata2 = copy.copy(metadata)

        # Labels will match, but be reordered
        labels = _generate_simple_map(
            min_size=2, max_size=15, min_value_length=5, max_value_length=20
        )
        metadata["labels"] = labels
        label_keys = list(labels.keys())
        random.shuffle(label_keys)
        metadata2["labels"] = {key: labels[key] for key in label_keys}

        # Annotations will match, but be reordered
        annotations = _generate_simple_map(
            min_size=2, max_size=15, min_value_length=5, max_value_length=20
        )
        metadata["annotations"] = annotations
        annotations_keys = list(annotations.keys())
        random.shuffle(annotations_keys)
        metadata2["annotations"] = {key: annotations[key] for key in annotations_keys}

        # Spec will match, but the top-level structure will be reordered.
        spec = _generate_random_dict(
            size=10, max_depth=3, min_value_length=5, max_value_length=20
        )
        spec_keys = list(spec.keys())
        random.shuffle(spec_keys)
        spec2 = {key: spec[key] for key in spec_keys}

        self.assertEqual(
            _hash_resource({"metadata": metadata, "spec": spec}),
            _hash_resource({"metadata": metadata2, "spec": spec2}),
        )
