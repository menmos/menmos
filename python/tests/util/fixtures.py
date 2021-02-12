import time
from pathlib import Path
from re import T
from typing import Iterator, Tuple

import pytest
from menmos.directory import DirectoryNode
from menmos.storage import StorageNode


def find_bin_path() -> Path:
    return Path("..").expanduser() / "target" / "debug"


@pytest.fixture
def node_directory() -> Iterator[DirectoryNode]:
    node = None
    try:
        node = DirectoryNode(find_bin_path() / "menmosd")
        yield node
    except:
        pass

    if node:
        node.stop()


@pytest.fixture
def directory_and_storage() -> Iterator[Tuple[DirectoryNode, StorageNode]]:
    directory = None
    storage = None
    try:
        directory = DirectoryNode(find_bin_path() / "menmosd")
        storage = StorageNode(find_bin_path() / "amphora")
        yield (directory, storage)
    except:
        pass

    try:
        if storage:
            storage.stop()
    except:
        pass

    if directory:
        directory.stop()
