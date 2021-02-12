import os
import subprocess
import time
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, List

import requests
from requests import Request


class Process:
    def __init__(self, binary_path: Path, arguments: List[str], port: int) -> None:
        self._binary_path = binary_path
        self._arguments = arguments

        self.host = f"http://localhost:{port}"

        self._session = requests.Session()
        self._session.headers.update({"Authorization": "test"})

        self._process: Any = None

    def _req(self, r: Request, allow_redirects: bool = True) -> Dict[str, Any]:
        prepared = self._session.prepare_request(r)
        resp = self._session.send(prepared, allow_redirects=allow_redirects)
        if resp.status_code != HTTPStatus.OK and resp.status_code != HTTPStatus.TEMPORARY_REDIRECT:
            print(resp.text)
            raise ValueError(f"unexpected status: {resp.status_code}")
        return resp.json()

    def is_healthy(self) -> bool:
        r = Request(method="GET", url=f"{self.host}/health")
        data = self._req(r)
        return data.get("message") == "healthy"

    def start(self, *args: Any) -> None:
        """
        Starts the process.
        """
        process_env = os.environ.copy()
        # process_env["RUST_LOG"] = "debug"
        self._process = subprocess.Popen([str(self._binary_path), *self._arguments, *args], env=process_env)
        self._wait_until_healthy()

    def stop(self) -> None:
        """
        Stops the process.
        """
        self._process.send_signal(2)
        self._process.wait()

        # TODO: There's an issue we didn't pinpoint where there's a race condition when running on slow machines.
        # Waiting a bit after the test mitigates this.
        time.sleep(2)

    def _wait_until_healthy(self) -> None:
        ts = time.time()
        while True:
            try:
                if self.is_healthy():
                    break
            except Exception:
                time.sleep(0.5)

            if time.time() - ts > 10:
                raise ConnectionRefusedError
