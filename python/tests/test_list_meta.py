from typing import Iterator, Tuple

import pytest
from _pytest.assertion import pytest_assertrepr_compare
from menmos.directory import DirectoryNode
from menmos.storage import StorageNode

from .util.fixtures import directory_and_storage
from .util.helpers import Document, with_data


@pytest.fixture
def basic_node(directory_and_storage: Tuple[DirectoryNode, StorageNode]) -> Iterator[Tuple[DirectoryNode, StorageNode]]:
    (directory, storage) = directory_and_storage
    docs = [
        Document("asdf", meta={"extension": "txt", "key": "yeet"}),
        Document("asdf", meta={"extension": "txt", "key": "yeet"}),
        Document("asdf", meta={"extension": "txt", "key": "yeet"}),
        Document("asdf", meta={"extension": "jpg", "key": "yeet"}),
        Document("asdf", meta={"extension": "jpg", "key": "yeet"}),
        Document("asdf", meta={"extension": "png", "key": "yeet"}),
    ]

    with_data(directory, docs)

    yield (directory, storage)


def test_list_meta_basic(basic_node: Tuple[DirectoryNode, StorageNode]) -> None:
    (directory, storage) = basic_node
    meta_info = directory.list_metadata()
    assert meta_info["meta"]["extension"] == {"txt": 3, "jpg": 2, "png": 1}
    assert meta_info["meta"]["key"] == {"yeet": 6}
