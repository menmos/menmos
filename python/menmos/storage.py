import json
import tempfile
from pathlib import Path
from typing import Any, Dict

from .process import Process


class StorageNode(Process):
    def __init__(
        self,
        binary_path: Path,
        port: int = 3031,
        name: str = "alpha",
        directory_port: int = 3030,
    ) -> None:
        self._temp_dir = tempfile.mkdtemp()
        self._data_dir = Path(self._temp_dir)

        config_path = self._data_dir / "config.json"
        with open(config_path, "a") as outfile:
            json.dump(self._build_config(name, port, directory_port), outfile)

        super().__init__(binary_path, ["--cfg", str(config_path)], port)

        self.start()

    def _build_config(
        self, name: str, port: int, directory_port: int
    ) -> Dict[str, Any]:
        cert_path = self._data_dir / "certs"
        cert_path.mkdir()

        return {
            "directory": {"url": "http://localhost", "port": directory_port},
            "node": {
                "name": name,
                "db_path": str(self._data_dir / "db/"),
                "registration_secret": "test",
                "admin_password": "test",
                "encryption_key": "t1fhrIw48oLxhJavFY5GRbrANiI9uBL8",
                "blob_storage": {
                    "type": "Directory",
                    "path": str(self._data_dir / "blobs"),
                },
            },
            "server": {
                "certificate_storage_path": str(cert_path),
                "subnet_mask": "255.255.255.0",
                "port": port,
            },
        }
