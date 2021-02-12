import base64
import json
import tempfile
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, List, Union

from requests import Request
from requests_toolbelt.multipart.encoder import MultipartEncoder

from .process import Process


class DirectoryNode(Process):
    def __init__(self, binary_path: Path, port: int = 3030):
        self._temp_dir = tempfile.mkdtemp()
        self._data_dir = Path(self._temp_dir)

        config_path = self._data_dir / "config.json"
        with open(config_path, "a") as outfile:
            json.dump(self._build_config(port), outfile)

        super().__init__(binary_path, ["--cfg", str(config_path)], port)

        self.start()

    def _build_config(self, port: int) -> Dict[str, Any]:
        return {
            "node": {
                "db_path": str(self._data_dir / "db/"),
                "registration_secret": "test",
                "admin_password": "test",
                "encryption_key": "t1fhrIw48oLxhJavFY5GRbrANiI9uBL8",
            },
            "server": {"type": "HTTP", "port": 3030},
        }

    def list_storage_nodes(self) -> Dict[str, Any]:
        r = Request(method="GET", url=f"{self.host}/node/storage")
        return self._req(r)

    def list_metadata(self, tags=None, meta_keys=None) -> Dict[str, Any]:
        r = Request(
            method="GET",
            url=f"{self.host}/metadata",
            headers={"content-type": "application/json"},
            json={"tags": tags, "meta_keys": meta_keys},
        )
        return self._req(r)

    def push(
        self,
        path: Path,
        size: int,
        name: str = None,
        tags: List[str] = None,
        meta: Dict[str, str] = None,
        parents: List[str] = None,
        blob_type: str = "File",
        allow_redirects: bool = True,
    ) -> Dict[str, Any]:

        if tags is None:
            tags = []

        if meta is None:
            meta = {}

        if parents is None:
            parents = []

        meta_dict = {
            "tags": tags,
            "metadata": meta,
            "parents": parents,
            "size": size,
            "name": name or str(path.name),
            "blob_type": blob_type,
        }
        dumped_meta = json.dumps(meta_dict)
        print(dumped_meta)

        status: Union[int, HTTPStatus] = HTTPStatus.TEMPORARY_REDIRECT
        url = f"{self.host}/blob"
        while status == HTTPStatus.TEMPORARY_REDIRECT:

            encoder = MultipartEncoder(
                fields={
                    "src": (
                        str(path),
                        open(str(path), "rb"),
                        "application/octet-stream",
                    )
                }
            )
            r = Request(
                method="POST",
                url=url,
                data=encoder,
                headers={
                    "x-blob-meta": base64.b64encode(dumped_meta.encode("utf-8")).decode(
                        "ascii"
                    ),
                    "authorization": "test",
                    "content-type": encoder.content_type,
                },
            )
            prepared = self._session.prepare_request(r)
            resp = self._session.send(prepared, allow_redirects=False)
            status = resp.status_code
            if resp.status_code == HTTPStatus.TEMPORARY_REDIRECT:
                if not allow_redirects:
                    raise ValueError(f"unexpected status: {resp.status_code}")
                else:
                    url = resp.headers["Location"]
                    continue

            if resp.status_code != HTTPStatus.OK:
                print(resp.text)
                raise ValueError(f"unexpected status: {resp.status_code}")

            return resp.json()

        return {}  # to make mypy happy

    def query(
        self,
        expression: str = None,
        start_from: int = 0,
        size: int = 30,
        sign_urls: bool = True,
    ) -> Dict[str, Any]:
        r = Request(
            method="POST",
            url=f"{self.host}/query",
            headers={"content-type": "application/json"},
            json={
                "expression": expression,
                "from": start_from,
                "size": size,
                "sign_urls": sign_urls,
            },
        )
        resp = self._req(r)
        return resp

    def delete(self, blob_id: str) -> None:
        status: Union[int, HTTPStatus] = HTTPStatus.TEMPORARY_REDIRECT
        url = f"{self.host}/blob/{blob_id}"
        while status == HTTPStatus.TEMPORARY_REDIRECT:
            r = Request(method="DELETE", url=url)
            prepared = self._session.prepare_request(r)
            resp = self._session.send(prepared, allow_redirects=False)
            status = resp.status_code
            if resp.status_code == HTTPStatus.TEMPORARY_REDIRECT:
                url = resp.headers["Location"]
                continue

            if resp.status_code != HTTPStatus.OK:
                print(resp.text)
                raise ValueError(f"unexpected status: {resp.status_code}")

            return resp.json()
