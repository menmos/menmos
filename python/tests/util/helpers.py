from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List

from menmos.directory import DirectoryNode


@dataclass
class Document:
    body: str
    meta: Dict[str, str] = field(default_factory=lambda: {})
    tags: List[str] = field(default_factory=lambda: [])
    parents: List[str] = field(default_factory=lambda: [])


def with_data(directory: DirectoryNode, docs: List[Document]) -> None:
    with TemporaryDirectory() as tmp_dir:
        for doc in docs:
            # create the temp file.
            path = Path(tmp_dir) / "tmp.txt"
            path.write_text(doc.body)
            directory.push(
                path,
                size=len(doc.body),
                tags=doc.tags,
                meta=doc.meta,
                parents=doc.parents,
            )
