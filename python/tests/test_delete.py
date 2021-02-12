import tempfile
from pathlib import Path
from typing import Tuple

import requests
from menmos.directory import DirectoryNode
from menmos.storage import StorageNode

from .util.fixtures import directory_and_storage


def test_delete_simple(directory_and_storage: Tuple[DirectoryNode, StorageNode]) -> None:
    (directory, storage) = directory_and_storage

    # Create a temp file.
    filename = tempfile.mktemp()
    with open(filename, "a") as outfile:
        outfile.write("THIS IS A TEST")

    # Upload the item.
    blob_id = directory.push(Path(filename), size=len("THIS IS A TEST"))["id"]

    # Make sure the item was registered in the directory.
    results = directory.query()
    assert results["total"] == 1
    assert results["hits"][0]["id"] == blob_id

    # Delete the item.
    directory.delete(blob_id)

    # Do the query again.
    results = directory.query()
    assert results["total"] == 0
    assert results["hits"] == []
