""" Tests the indexing capabilities of the nodes. """

import tempfile
from http import HTTPStatus
from pathlib import Path
from typing import Tuple

import requests
from menmos.directory import DirectoryNode
from menmos.storage import StorageNode

from .util.fixtures import directory_and_storage


def test_put_simple_loop(directory_and_storage: Tuple[DirectoryNode, StorageNode]) -> None:
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

    # Make sure we can get the file.
    item_url = results["hits"][0]["url"]
    resp = requests.get(item_url)
    assert resp.status_code == HTTPStatus.OK

    # Validate the file contents.
    assert resp.text == "THIS IS A TEST"


def test_put_without_ack(directory_and_storage: Tuple[DirectoryNode, StorageNode]) -> None:
    """
    Tests that a put without an acknowledgement from the targeted storage node will not
    store the item metadata.
    """
    (directory, storage) = directory_and_storage

    # Create a temp file.
    filename = tempfile.mktemp()
    with open(filename, "a") as outfile:
        outfile.write("THIS IS A TEST")

    # Upload the item.
    did_except = False
    try:
        directory.push(Path(filename), size=len("THIS IS A TEST"), allow_redirects=False)
    except:
        did_except = True

    assert did_except
    results = directory.query()
    assert results["total"] == 0
    assert results["hits"] == []
