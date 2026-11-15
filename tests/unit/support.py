"""Shared helpers of the unit tests (tests/unit/test_*.py).

The unit tests run inside the built image, as the app user plotly (uid 30000), in one
container with --network none (see run_unit.py and tests/run.sh). The app is imported from
/app; tests/run.sh puts a dummy env file at /app/secrets/.env.production and sets
APP_ENV=production, because config.py needs one at import time. With APP_SRC set,
tests/run.sh copies the checkout's app/ over /app first, so the checkout's code is tested
without a rebuild.

Every test gets its own stub API (tests/fake_api.py) on 127.0.0.1 in a thread, and the
backend settings of the app pointed at it (root URL form, .../mdb; self.v1_url is the
production form .../mdb/api/v1), with no retries and no backoff unless a test sets them, and
the connector's metadata caches emptied. No test depends on another.
"""
import os
import socket
import sys
import threading
import unittest
from pathlib import Path
from unittest import mock

TESTS_DIR = Path(__file__).resolve().parent.parent
APP_DIR = Path(os.environ.get("NUKI_APP_DIR", "/app"))
for p in (str(TESTS_DIR), str(APP_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import config  # noqa: E402
import db_connector  # noqa: E402
import fake_api  # noqa: E402

FIXTURES = TESTS_DIR / "fixtures"


class ApiTestCase(unittest.TestCase):
    """Base class: a fresh stub API per test, and the app's settings pointed at it."""

    def setUp(self):
        self.server = fake_api.make_server(FIXTURES, 0, "127.0.0.1")
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.root_url = f"http://127.0.0.1:{self.server.server_port}/mdb"
        self.v1_url = self.root_url + "/api/v1"
        self.base_url = self.root_url
        self.set_settings(
            backend_api_base_url=self.base_url,
            backend_api_timeout_seconds=5.0,
            backend_api_max_retries=0,
            backend_api_backoff_factor=0.0,
            backend_api_max_parallel=2,
            backend_api_raw_chunk_years=1,
        )
        db_connector.clear_caches()
        self.addCleanup(db_connector.clear_caches)

    def set_settings(self, **values):
        for key, value in values.items():
            patcher = mock.patch.object(config.settings, key, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def api_requests(self, endpoint):
        """Query parameters of the requests to <endpoint>, a path suffix ("metadata": v1 and
        v2, "v2/metadata", "measurements/raw")."""
        return [params for path, params in self.server.requests if path.endswith("/" + endpoint)]


def silent_port():
    """A listening TCP socket on 127.0.0.1 that never answers (connect succeeds, the read
    times out). Returns (socket, port); the caller closes the socket."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(8)
    return sock, sock.getsockname()[1]


def closed_port():
    """A port on 127.0.0.1 with nothing listening (connection refused)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port
