import tempfile
from http import HTTPStatus
from typing import Tuple

import requests
from menmos.directory import DirectoryNode
from menmos.storage import StorageNode

from .util.fixtures import directory_and_storage, node_directory


def test_directory_node_startup(node_directory: DirectoryNode) -> None:
    assert node_directory.is_healthy()


def test_directory_node_initially_empty(node_directory: DirectoryNode) -> None:
    resp = node_directory.query()
    assert resp["total"] == 0


def test_directory_node_listens_for_storage_nodes(directory_and_storage: Tuple[DirectoryNode, StorageNode]) -> None:
    (directory, _storage) = directory_and_storage
    nodes = directory.list_storage_nodes()
    assert len(nodes.get("storage_nodes", [])) == 1
