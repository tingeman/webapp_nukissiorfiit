"""Unit tests of the phase 2 API client (app/v2/api_client.py) and the ported app/v2/rawseries.py
and trumpet.py, against the stub monitoring_db API (tests/fake_api.py): root URL, retries and
errors, metadata caches, the mast catalogue, daily/raw chunks, parallelism and the deadline,
the data shape, the column selection and the thinning. The fixture data is described in
tests/fixtures/make_fixtures.py. Each test's docstring is its name in the test output
("unit: v2_api_client: <docstring>")."""
import ast
import datetime as dt
import os
import threading
import time
import unittest
from unittest import mock

import numpy as np
import pydantic

from support import APP_DIR, ApiTestCase, closed_port, config, fake_api, silent_port
from v2 import api_client, rawseries, texts, trumpet
from v2.api_client import ApiError, Client

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
TEKBOX_M6 = "deveui:feedbeefcafe0002"          # device 10, 2026-09-01..05, offset 0
CAMPBELL_M6 = "serial:SIS2024-01:12096"        # device 11, 2026-10-01..05, offset 1
TEKBOX_M9 = "deveui:feedbeefcafe0001"          # device 20, 2024-10-14..18, offset 2
INTERNAL = (500, {"error": {"code": "INTERNAL_ERROR", "message": "boom", "details": {}}})


def utc(*args):
    return dt.datetime(*args, tzinfo=UTC)


def ms(s):
    return np.datetime64(s, "ms")


class ClientTestCase(ApiTestCase):
    """ApiTestCase plus a client from the settings (backend pointed at the stub)."""

    def setUp(self):
        super().setUp()
        self.set_settings(backend_api_daily_chunk_years=3)
        self.client = Client.from_settings(config.settings)

    def counting(self, delay=0.0, delays=None):
        """Patch client._get: counts the requests in flight (self.peak) and sleeps a while."""
        self.active, self.peak, lock = [0], [0], threading.Lock()
        real = self.client._get

        def slow_get(path, params=None):
            with lock:
                self.active[0] += 1
                self.peak[0] = max(self.peak[0], self.active[0])
            try:
                time.sleep((delays or {}).get((params or {}).get("start"), delay))
                return real(path, params)
            finally:
                with lock:
                    self.active[0] -= 1
        patcher = mock.patch.object(self.client, "_get", slow_get)
        patcher.start()
        self.addCleanup(patcher.stop)

    def bounds(self, endpoint):
        return [(p["start"], p["end"]) for p in self.api_requests(endpoint)]


# ---- configuration and structure --------------------------------------------------------------

class ConfigTest(unittest.TestCase):
    def test_defaults(self):
        """BACKEND_API_DAILY_CHUNK_YEARS defaults to 3 and DOWNLOAD_TIME_BUDGET_S to 75"""
        fields = config.ConfigSettings.model_fields
        self.assertEqual(fields["backend_api_daily_chunk_years"].default, 3)
        self.assertEqual(fields["download_time_budget_s"].default, 75.0)

    def test_env_values(self):
        """BACKEND_API_DAILY_CHUNK_YEARS and DOWNLOAD_TIME_BUDGET_S are read from env; 0 fails"""
        with mock.patch.dict(os.environ, {"BACKEND_API_DAILY_CHUNK_YEARS": "2", "DOWNLOAD_TIME_BUDGET_S": "25.5"}):
            s = config.ConfigSettings()
        self.assertEqual((s.backend_api_daily_chunk_years, s.download_time_budget_s), (2, 25.5))
        for name in ("BACKEND_API_DAILY_CHUNK_YEARS", "DOWNLOAD_TIME_BUDGET_S"):
            with self.subTest(name=name), mock.patch.dict(os.environ, {name: "0"}):
                with self.assertRaises(pydantic.ValidationError):
                    config.ConfigSettings()

    def test_no_db_connector_import(self):
        """no module of app/v2/ imports db_connector (or the phase 1 app)"""
        for path in sorted((APP_DIR / "v2").glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            names = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names |= {a.name for a in node.names}
                elif isinstance(node, ast.ImportFrom):
                    names |= {node.module or ""} | {a.name for a in node.names}
            with self.subTest(module=path.name):
                self.assertFalse(names & {"db_connector", "app"}, names)


class FromSettingsTest(ClientTestCase):
    def test_from_settings(self):
        """from_settings takes the root URL, timeout, retries, parallelism, chunk years and the station pattern"""
        self.set_settings(backend_api_max_parallel=3, backend_api_raw_chunk_years=2,
                          backend_api_daily_chunk_years=4, backend_api_max_retries=5,
                          nuki_station_pattern="QAA.*")
        c = Client.from_settings(config.settings)
        self.assertEqual((c.base_url, c.timeout_s, c.max_retries, c.max_parallel, c.raw_chunk_years,
                          c.daily_chunk_years), (self.root_url, 5.0, 5, 3, 2, 4))
        self.assertTrue(c.station_regex.fullmatch("QAA2017-01"))
        self.assertEqual(self.server.requests, [])     # nothing is fetched at construction


# ---- root URL and transport -------------------------------------------------------------------

class RootUrlTest(ClientTestCase):
    def test_root_form(self):
        """the root URL form and a trailing slash are used as they are, without a warning"""
        for url in (self.root_url, self.root_url + "/"):
            with self.assertNoLogs("nuki.v2.api", level="WARNING"):
                self.assertEqual(Client(url).base_url, self.root_url)

    def test_suffix_stripped(self):
        """a trailing /api/v1, /api/v2 or /api/v1/ is stripped, with one WARNING at construction"""
        for url in (self.root_url + "/api/v1", self.root_url + "/api/v2", self.root_url + "/api/v1/"):
            with self.assertLogs("nuki.v2.api", level="WARNING") as logs:
                c = Client(url)
            self.assertEqual((c.base_url, len(logs.records)), (self.root_url, 1))
            self.assertIn("API version", logs.output[0])

    def test_production_form_paths(self):
        """with the production form .../mdb/api/v1 the data comes from <root>/api/v2/measurements/..."""
        c = Client(self.v1_url)
        c.daily(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW)
        c.raw(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW)
        c.metadata_v1()
        self.assertEqual([p for p, _ in self.server.requests],
                         ["/mdb/api/v2/measurements/daily", "/mdb/api/v2/measurements/raw", "/mdb/api/v1/metadata"])


class RetryTest(ClientTestCase):
    def test_retry_config(self):
        """the session retries connection errors only (connect=max_retries, no read, status or other)"""
        with Client(self.root_url, max_retries=3)._session() as session:
            retry = session.get_adapter("http://x/").max_retries
        self.assertEqual(retry.connect, 3)
        self.assertIs(retry.read, False)
        self.assertEqual((retry.status, retry.other), (0, 0))
        self.assertFalse(retry.status_forcelist)

    def test_504(self):
        """a 504 fails after one request, also with retries configured, with the timeout text and one WARNING"""
        c = Client(self.root_url, max_retries=2)
        self.server.force = fake_api.QUERY_TIMEOUT
        with self.assertLogs("nuki.v2.api", level="WARNING") as logs:
            with self.assertRaises(ApiError) as cm:
                c.daily(TEKBOX_M6, "2026-09-01", "2026-09-05", now=NOW)
        self.assertEqual(str(cm.exception), texts.API_TIMEOUT)
        self.assertIn("504", cm.exception.reason)
        self.assertIn("QUERY_TIMEOUT", cm.exception.reason)
        self.assertEqual((len(self.server.requests), len(logs.records)), (1, 1))

    def test_read_timeout(self):
        """an API that does not answer within the timeout fails after one connection with the timeout text"""
        sock, port = silent_port()
        self.addCleanup(sock.close)
        connections, stop = [], threading.Event()

        def accept():
            sock.settimeout(0.05)
            while not stop.is_set():
                try:
                    connections.append(sock.accept()[0])
                except OSError:
                    continue
        thread = threading.Thread(target=accept, daemon=True)
        thread.start()
        self.addCleanup(lambda: [c.close() for c in connections])
        self.addCleanup(thread.join)
        self.addCleanup(stop.set)
        c = Client(f"http://127.0.0.1:{port}/mdb", timeout_s=0.5, max_retries=2, backoff_factor=0)
        with self.assertRaises(ApiError) as cm:
            c.raw(TEKBOX_M6, "2026-09-01", "2026-09-05", now=NOW)
        self.assertEqual(str(cm.exception), texts.API_TIMEOUT)
        time.sleep(0.2)
        self.assertEqual(len(connections), 1)

    def test_connection_refused(self):
        """a refused connection is retried and then fails with the unreachable text"""
        c = Client(f"http://127.0.0.1:{closed_port()}/mdb", max_retries=2, backoff_factor=0)
        original = api_client.Retry.increment
        with mock.patch.object(api_client.Retry, "increment", autospec=True, side_effect=original) as increment:
            with self.assertRaises(ApiError) as cm:
                c.metadata_v1()
        self.assertEqual(str(cm.exception), texts.API_UNREACHABLE)
        self.assertEqual(increment.call_count, 3)    # the first try and 2 retries

    def test_http_errors(self):
        """a 404 or 500 gives the API error text with the API's message; invalid JSON the invalid text"""
        with self.assertRaises(ApiError) as cm:
            self.client.daily("deveui:0000000000000000", "2026-09-01", "2026-09-01", now=NOW)
        self.assertEqual(str(cm.exception),
                         texts.API_ERROR.format(detail="Recording device not found. (DEVEUI_NOT_FOUND, HTTP 404)"))
        self.server.force = INTERNAL
        with self.assertRaises(ApiError) as cm:
            self.client.raw(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW)
        self.assertIn("HTTP 500", str(cm.exception))
        for body in (b"not json", b"[1, 2]"):
            self.server.force = (200, body)
            with self.assertRaises(ApiError) as cm:
                self.client.raw(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW)
            self.assertEqual(str(cm.exception), texts.API_INVALID)

    def test_accept_gzip(self):
        """the /daily and /raw requests send Accept-Encoding with gzip"""
        self.client.daily(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW)
        self.client.raw(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW)
        for headers in self.server.headers:
            self.assertIn("gzip", headers.get("accept-encoding", ""))

    def test_invalid_logger(self):
        """an invalid logger value raises ValueError before any request"""
        for value in ("", "serial:12096", "deveui:", "station:SIS2024-01"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.client.daily(value, "2026-09-01", "2026-09-01", now=NOW)
        self.assertEqual(self.server.requests, [])


class ShapeTest(ClientTestCase):
    def test_raw_shape(self):
        """a /raw answer without aligned values or keyed columns fails with the invalid text"""
        good = {"time": ["2026-09-01T00:00:00Z"], "columns": [{"key": "a"}], "values": [[1.0]]}
        for bad in ({**good, "values": [[1.0, 2.0]]}, {**good, "values": []}, {**good, "columns": [{}]},
                    {k: v for k, v in good.items() if k != "time"}, {**good, "values": {"mean": [[1.0]]}}):
            self.server.force = (200, bad)
            with self.subTest(bad=bad), self.assertRaises(ApiError) as cm:
                self.client.raw(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW)
            self.assertEqual(str(cm.exception), texts.API_INVALID)
        self.server.force = (200, good)
        self.assertEqual(list(self.client.raw(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW).series), ["a"])

    def test_daily_shape(self):
        """a /daily answer without mean/min/max/count arrays aligned with time fails with the invalid text"""
        arr = [[1.0]]
        good = {"time": ["2026-09-01"], "columns": [{"key": "a"}],
                "values": {"mean": arr, "min": arr, "max": arr, "count": [[3]]}}
        for bad in ({**good, "values": arr}, {**good, "values": {"mean": arr, "min": arr, "max": arr}},
                    {**good, "values": {**good["values"], "count": [[3, 4]]}}):
            self.server.force = (200, bad)
            with self.subTest(bad=bad), self.assertRaises(ApiError) as cm:
                self.client.daily(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW)
            self.assertEqual(str(cm.exception), texts.API_INVALID)
        self.server.force = (200, good)
        s = self.client.daily(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW).series["a"]
        self.assertEqual((s.mean.tolist(), s.count.tolist()), ([1.0], [3]))


# ---- metadata caches and catalogue ------------------------------------------------------------

class MetadataCacheTest(ClientTestCase):
    def test_v1_cached(self):
        """v1 metadata is fetched once and then served from the cache"""
        self.assertIs(self.client.metadata_v1(), self.client.metadata_v1())
        self.assertEqual([p for p, _ in self.server.requests], ["/mdb/api/v1/metadata"])

    def test_expiry(self):
        """the v1 and v2 metadata caches expire after 10 minutes"""
        now = [1000.0]
        c = Client(self.root_url, clock=lambda: now[0])
        c.metadata_v1(), c.metadata_v2("SIS2024-01")
        now[0] += 599
        c.metadata_v1(), c.metadata_v2("SIS2024-01")
        now[0] += 2
        c.metadata_v1(), c.metadata_v2("SIS2024-01")
        self.assertEqual((len(self.api_requests("v1/metadata")), len(self.api_requests("v2/metadata"))), (2, 2))

    def test_failure_not_cached(self):
        """a failed metadata request is not cached (one WARNING): the next call asks again"""
        self.server.force = INTERNAL
        for call in (self.client.metadata_v1, lambda: self.client.metadata_v2("SIS2024-01")):
            with self.assertLogs("nuki.v2.api", level="WARNING") as logs, self.assertRaises(ApiError):
                call()
            self.assertEqual(len(logs.records), 1)
        self.server.force = None
        self.assertIn("stations", self.client.metadata_v1())
        self.assertIn("stations", self.client.metadata_v2("SIS2024-01"))
        self.assertEqual((len(self.api_requests("v1/metadata")), len(self.api_requests("v2/metadata"))), (2, 2))

    def test_v2_per_station(self):
        """v2 metadata is requested with station=<identifier> and cached per station"""
        a = self.client.metadata_v2("SIS2024-01")
        self.client.metadata_v2("SIS2024-01")
        self.client.metadata_v2("SIS2024-03")
        self.assertEqual(self.api_requests("v2/metadata"), [{"station": "SIS2024-01"}, {"station": "SIS2024-03"}])
        self.assertEqual([s["identifier"] for s in a["stations"]], ["SIS2024-01"])

    def test_masts_cached(self):
        """a second masts() call makes no request; clear_cache() empties both caches"""
        self.client.masts()
        n = len(self.server.requests)
        self.client.masts()
        self.assertEqual((len(self.server.requests), n), (n, 1 + 4))   # v1 + v2 of the 4 included stations
        self.client.clear_cache()
        self.client.masts()
        self.assertEqual(len(self.server.requests), 2 * n)


class CatalogueTest(ClientTestCase):
    def test_masts(self):
        """the catalogue lists Mast 6, 9 and 13 (numeric order), not QAA2017-01 or SIS2024-07"""
        masts = self.client.masts()
        self.assertEqual([m["name"] for m in masts], ["Mast 6", "Mast 9", "Mast 13"])
        idents = [i for m in masts for i in m["station_identifiers"]]
        self.assertFalse({"QAA2017-01", "SIS2024-07"} & set(idents))
        self.assertEqual({s["station"] for s in self.api_requests("v2/metadata")},
                         {"SIS2024-01", "SIS2024-03", "NUKI2026-M9", "SIS2024-05"})

    def test_mast6(self):
        """Mast 6 has the Tekbox (deveui) and the Campbell (station and serial), by first measurement"""
        mast = self.client.masts()[0]
        self.assertEqual(mast["station_identifiers"], ["SIS2024-01"])
        self.assertEqual(mast["loggers"], [
            {"value": TEKBOX_M6, "label": "Tekbox_TBSL1_3 (feedbeefcafe0002), 2026-09-01 to 2026-09-05",
             "station": "SIS2024-01", "first": "2026-09-01T00:00:00Z", "last": "2026-09-05T00:00:00Z"},
            {"value": CAMPBELL_M6, "label": "CR300_M6 (12096), 2026-10-01 to 2026-10-05",
             "station": "SIS2024-01", "first": "2026-10-01T00:00:00Z", "last": "2026-10-05T00:00:00Z"},
        ])

    def test_mast9_two_stations(self):
        """Mast 9 groups SIS2024-03 (Tekbox) and NUKI2026-M9 (Campbell) by station name"""
        mast = self.client.masts()[1]
        self.assertEqual(sorted(mast["station_identifiers"]), ["NUKI2026-M9", "SIS2024-03"])
        self.assertEqual([lg["value"] for lg in mast["loggers"]], [TEKBOX_M9, "serial:NUKI2026-M9:12215"])
        self.assertEqual([lg["station"] for lg in mast["loggers"]], ["SIS2024-03", "NUKI2026-M9"])

    def test_mast13_without_tekbox(self):
        """Mast 13 has only CR300_M13: the Tekbox without measurements is left out"""
        mast = self.client.masts()[2]
        self.assertEqual([lg["value"] for lg in mast["loggers"]], ["serial:SIS2024-05:12216"])
        self.assertTrue(mast["loggers"][0]["label"].startswith("CR300_M13 (12216), "))

    def test_custom_pattern(self):
        """a custom station pattern applies (fullmatch); a mast name without the prefix is kept"""
        c = Client(self.root_url, station_pattern=r"QAA\d{4}-01")
        masts = c.masts()
        self.assertEqual([m["name"] for m in masts], ["Qaanaaq borehole 1"])
        self.assertEqual([lg["value"] for lg in masts[0]["loggers"]], ["serial:QAA2017-01:A522C6"])
        self.assertEqual(Client(self.root_url, station_pattern="SIS2024-0").masts(), [])

    def test_mast_without_data(self):
        """a mast whose loggers have no measurements is left out"""
        self.server.fixtures["measurements"] = {k: v for k, v in self.server.fixtures["measurements"].items()
                                                if not str(k).startswith("41")}
        self.assertEqual(Client(self.root_url, station_pattern="SIS2024-05").masts(), [])

    def test_default_logger(self):
        """default_logger is the logger with the latest last measurement"""
        masts = self.client.masts()
        self.assertEqual([api_client.default_logger(m)["value"] for m in masts],
                         [CAMPBELL_M6, "serial:NUKI2026-M9:12215", "serial:SIS2024-05:12216"])
        self.assertIsNone(api_client.default_logger({"loggers": []}))

    def test_bad_metadata(self):
        """metadata of an unexpected shape fails with the invalid text"""
        self.server.force = (200, {"stations": "nope"})
        with self.assertLogs("nuki.v2.api", level="WARNING"), self.assertRaises(ApiError) as cm:
            self.client.masts()
        self.assertEqual(str(cm.exception), texts.API_INVALID)


# ---- chunks -----------------------------------------------------------------------------------

class ChunkTest(ClientTestCase):
    def test_daily_chunks_august(self):
        """daily chunks split at 1 August in pieces of 3 hydrological years, touching without overlap"""
        self.assertEqual(api_client.daily_chunks(dt.date(2020, 3, 1), dt.date(2026, 10, 5), 3, NOW), [
            ("2020-03-01T00:00:00Z", "2022-07-31T23:59:59Z"),
            ("2022-08-01T00:00:00Z", "2025-07-31T23:59:59Z"),
            ("2025-08-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertEqual(api_client.daily_chunks(dt.date(2024, 8, 1), dt.date(2025, 8, 1), 1, NOW), [
            ("2024-08-01T00:00:00Z", "2025-07-31T23:59:59Z"),
            ("2025-08-01T00:00:00Z", "2025-08-01T23:59:59Z")])

    def test_daily_requests(self):
        """daily() sends the chunk bounds of BACKEND_API_DAILY_CHUNK_YEARS (1 request for a logger's life)"""
        self.client.daily(TEKBOX_M9, "2024-10-14", "2026-10-05", now=NOW)
        self.assertEqual(self.api_requests("measurements/daily"),
                         [{"deveui": "feedbeefcafe0001", "start": "2024-10-14T00:00:00Z", "end": "2026-10-05T23:59:59Z"}])
        Client(self.root_url, daily_chunk_years=1).daily(TEKBOX_M9, "2024-10-14", "2026-10-05", now=NOW)
        self.assertEqual(sorted(self.bounds("measurements/daily")[1:]), [   # parallel: any arrival order
            ("2024-10-14T00:00:00Z", "2025-07-31T23:59:59Z"),
            ("2025-08-01T00:00:00Z", "2026-07-31T23:59:59Z"),
            ("2026-08-01T00:00:00Z", "2026-10-05T23:59:59Z")])

    def test_raw_chunks_from_start(self):
        """raw chunks are BACKEND_API_RAW_CHUNK_YEARS years counted from the start, not from 1 August"""
        self.client.raw(TEKBOX_M9, "2024-10-01", "2026-03-31", now=NOW)
        self.assertEqual(sorted(self.bounds("measurements/raw")), [
            ("2024-10-01T00:00:00Z", "2025-09-30T23:59:59Z"),
            ("2025-10-01T00:00:00Z", "2026-03-31T23:59:59Z")])
        self.assertEqual(api_client.raw_chunks(dt.date(2024, 2, 29), dt.date(2025, 6, 1), 1, NOW)[0],
                         ("2024-02-29T00:00:00Z", "2025-02-28T23:59:59Z"))

    def test_zoom_window(self):
        """datetimes (naive taken as UTC, strings with Z or an offset) give exactly start..end in one request"""
        self.client.raw(TEKBOX_M6, dt.datetime(2026, 9, 2, 6, 0, 0, 500), "2026-09-02T12:30:00+02:00", now=NOW)
        self.client.raw(TEKBOX_M6, "2026-09-02T06:00:00Z", utc(2026, 9, 2, 10, 30), now=NOW)
        self.assertEqual(self.bounds("measurements/raw"), [("2026-09-02T06:00:00Z", "2026-09-02T10:30:00Z")] * 2)

    def test_now_and_future(self):
        """chunks end at now; a period after now or a reversed one makes no request and no series"""
        self.assertEqual(api_client.raw_chunks(dt.date(2026, 11, 1), dt.date(2026, 12, 1), 1, NOW),
                         [("2026-11-01T00:00:00Z", "2026-11-15T12:00:00Z")])
        result = self.client.daily(TEKBOX_M6, "2027-01-01", "2027-02-01", now=NOW)
        reversed_ = self.client.raw(TEKBOX_M6, "2026-09-05", "2026-09-01", now=NOW)
        self.assertEqual((result.series, result.requests, reversed_.series), ({}, [], {}))
        self.assertEqual(self.server.requests, [])


# ---- parallelism, errors and the deadline -----------------------------------------------------

class FetchTest(ClientTestCase):
    def test_max_parallel(self):
        """at most max_parallel chunk requests are in flight (2, then 1), results in time order"""
        c = self.client = Client(self.root_url, max_parallel=2, raw_chunk_years=1)
        self.counting(delays={"2022-10-16T00:00:00Z": 0.4, "2023-10-16T00:00:00Z": 0.2})
        result = c.raw(TEKBOX_M9, "2022-10-16", "2025-10-15", now=NOW)   # 3 chunks, the first answers last
        self.assertEqual((len(result.requests), self.peak[0]), (3, 2))
        self.assertEqual([r.start for r in result.requests],
                         ["2022-10-16T00:00:00Z", "2023-10-16T00:00:00Z", "2024-10-16T00:00:00Z"])
        c = self.client = Client(self.root_url, max_parallel=1)
        self.counting(delay=0.05)
        c.raw(TEKBOX_M9, "2022-10-16", "2025-10-15", now=NOW)
        self.assertEqual(self.peak[0], 1)

    def test_error_drops_queued(self):
        """the first failing chunk raises its ApiError (one WARNING) and the queued chunks are not sent"""
        c = Client(self.root_url, max_parallel=1, raw_chunk_years=1)
        self.server.force = INTERNAL
        with self.assertLogs("nuki.v2.api", level="WARNING") as logs, self.assertRaises(ApiError):
            c.raw(TEKBOX_M9, "2020-01-01", "2025-12-31", now=NOW)    # 6 chunks
        time.sleep(0.2)
        self.assertLess(len(self.server.requests), 6)
        self.assertEqual(len(logs.records), 1)

    def test_deadline(self):
        """with a deadline no chunk is started after it: the call fails with the download-too-large text"""
        now = [0.0]
        c = self.client = Client(self.root_url, max_parallel=1, raw_chunk_years=1, clock=lambda: now[0])
        real = c._get

        def get_10s(path, params=None):      # every request takes 10 s on the client's clock
            now[0] += 10
            return real(path, params)
        with mock.patch.object(c, "_get", get_10s):
            with self.assertLogs("nuki.v2.api", level="WARNING") as logs, self.assertRaises(ApiError) as cm:
                c.raw(TEKBOX_M9, "2022-10-16", "2025-10-15", now=NOW, deadline=c.deadline_in(15))
            self.assertEqual(str(cm.exception), texts.DOWNLOAD_TOO_LARGE)
            self.assertIn("time budget", cm.exception.reason)
            self.assertEqual(len(logs.records), 1)
            self.assertEqual(len(self.api_requests("measurements/raw")), 2)    # started at 0 s and 10 s
            with self.assertRaises(ApiError):
                c.daily(TEKBOX_M9, "2024-10-14", "2024-10-18", now=NOW, deadline=c.deadline_in(0))
            self.assertEqual(self.api_requests("measurements/daily"), [])
            result = c.raw(TEKBOX_M9, "2022-10-16", "2025-10-15", now=NOW, deadline=c.deadline_in(100))
        self.assertEqual(len(result.requests), 3)

    def test_timing(self):
        """each request has a RequestTiming (bounds, seconds, bytes); the result sums them"""
        result = Client(self.root_url, raw_chunk_years=1).raw(TEKBOX_M6, "2025-09-03", "2026-10-05", now=NOW)
        self.assertEqual([(r.start, r.end) for r in result.requests],
                         [("2025-09-03T00:00:00Z", "2026-09-02T23:59:59Z"),
                          ("2026-09-03T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertTrue(all(r.bytes > 0 and r.seconds >= 0 for r in result.requests))
        self.assertEqual(result.bytes, sum(r.bytes for r in result.requests))
        self.assertGreaterEqual(result.wall_seconds, 0)
        self.assertAlmostEqual(result.request_seconds, sum(r.seconds for r in result.requests))


# ---- data shape -------------------------------------------------------------------------------

class RawDataTest(ClientTestCase):
    def test_tekbox(self):
        """raw: one RawSeries per column with details and datetime64[ms]/float arrays of the fixture values"""
        result = self.client.raw(TEKBOX_M6, "2026-09-01", "2026-09-05", now=NOW)
        self.assertEqual(self.api_requests("measurements/raw"),
                         [{"deveui": "feedbeefcafe0002", "start": "2026-09-01T00:00:00Z", "end": "2026-09-05T23:59:59Z"}])
        self.assertEqual(len(result.series), 11)
        s = result.series["GroundTemp02__GroundTemperature"]
        self.assertEqual((s.details["sensor_name"], s.details["relative_z"], s.details["unit"]),
                         ("GroundTemp02", -0.25, "C"))
        self.assertEqual((s.time.dtype, s.value.dtype, len(s.time)), (np.dtype("datetime64[ms]"), np.dtype(float), 33))
        self.assertEqual(s.time[10], ms("2026-09-02T06:00"))
        self.assertAlmostEqual(s.value[10], -0.2 - 0.002 * 10)

    def test_campbell_gaps(self):
        """raw: a Campbell is selected by station and serial; each column keeps only its own points"""
        result = self.client.raw(CAMPBELL_M6, "2026-10-01", "2026-10-01", now=NOW)
        self.assertEqual([(p["station"], p["serial"]) for p in self.api_requests("measurements/raw")],
                         [("SIS2024-01", "12096")])
        x = result.series["12096__Dual Axis Inclinometer__XAngle"]
        battery = result.series["12096__BatteryVoltage__BatteryVoltage"]
        self.assertEqual((len(battery.time), len(x.time)), (8, 4))     # 3-hourly and 6-hourly
        self.assertEqual(x.time[1], ms("2026-10-01T06:00"))
        self.assertAlmostEqual(x.value[1], 0.452)

    def test_across_chunks(self):
        """raw: a series split over two chunks is joined in time order"""
        result = Client(self.root_url, raw_chunk_years=1).raw(TEKBOX_M6, "2025-09-03", "2026-10-05", now=NOW)
        s = result.series["BatteryVoltage__BatteryVoltage"]
        self.assertEqual((len(result.payloads), len(s.time)), (2, 33))
        self.assertTrue((np.diff(s.time) > np.timedelta64(0, "ms")).all())
        self.assertEqual((s.time[0], s.time[-1]), (ms("2026-09-01T00:00"), ms("2026-09-05T00:00")))


class DailyDataTest(ClientTestCase):
    def test_tekbox(self):
        """daily: one DailySeries per column with day, mean, min, max and count arrays"""
        result = self.client.daily(TEKBOX_M6, "2026-09-01", "2026-09-05", now=NOW)
        s = result.series["BatteryVoltage__BatteryVoltage"]
        self.assertEqual(s.time.tolist(), [dt.date(2026, 9, d) for d in range(1, 6)])
        self.assertEqual(s.count.tolist(), [8, 8, 8, 8, 1])
        self.assertAlmostEqual(s.mean[0], 3.7 - 0.001 * 3.5)
        self.assertEqual((s.min[0], s.max[0], s.mean[4]), (3.693, 3.7, 3.668))
        self.assertEqual(s.details["timeseries_name"], "BatteryVoltage")
        self.assertEqual(set(result.series), set(self.client.raw(TEKBOX_M6, "2026-09-01", "2026-09-05", now=NOW).series))

    def test_campbell_counts(self):
        """daily: the 6-hourly Campbell inclinometer has 4 measurements per day"""
        result = self.client.daily(CAMPBELL_M6, "2026-10-01", "2026-10-02", now=NOW)
        x = result.series["12096__Dual Axis Inclinometer__XAngle"]
        self.assertEqual(x.count.tolist(), [4, 4])
        self.assertAlmostEqual(x.mean[0], 0.453)

    def test_day_in_two_chunks(self):
        """daily: the same day in two payloads is merged (counts summed, mean weighted, min of min, max of max)"""
        col = {"key": "a"}
        p1 = {"time": ["2026-09-01", "2026-09-02"], "columns": [col],
              "values": {"mean": [[1.0, 2.0]], "min": [[0.5, 1.0]], "max": [[1.5, 3.0]], "count": [[2, 4]]}}
        p2 = {"time": ["2026-09-02", "2026-09-03"], "columns": [col],
              "values": {"mean": [[5.0, None]], "min": [[4.0, None]], "max": [[6.0, None]], "count": [[4, 0]]}}
        s = api_client._daily_series([p1, p2])["a"]
        self.assertEqual(s.time.tolist(), [dt.date(2026, 9, 1), dt.date(2026, 9, 2)])
        self.assertEqual((s.mean.tolist(), s.min.tolist(), s.max.tolist(), s.count.tolist()),
                         ([1.0, 3.5], [0.5, 1.0], [1.5, 6.0], [2, 8]))


# ---- column selection -------------------------------------------------------------------------

def synthetic(columns, kind="raw"):
    """{key: series} without HTTP; columns is {key: details}; values 1, 2, 3."""
    t = np.array(["2026-10-01T00:00", "2026-10-01T03:00", "2026-10-01T06:00"], dtype="datetime64[ms]")
    v = np.array([1.0, 2.0, 3.0])
    out = {}
    for k, d in columns.items():
        details = dict(d, key=k)
        if kind == "raw":
            out[k] = api_client.RawSeries(details, t, v)
        else:
            out[k] = api_client.DailySeries(details, t.astype("datetime64[D]"), v, v - 0.5, v + 0.5,
                                            np.array([8, 8, 8]))
    return out


class SelectionTest(ClientTestCase):
    def setUp(self):
        super().setUp()
        self.tekbox = self.client.raw(TEKBOX_M6, "2026-09-01", "2026-09-01", now=NOW).series
        self.campbell = self.client.daily(CAMPBELL_M6, "2026-10-01", "2026-10-01", now=NOW).series

    def test_battery(self):
        """battery() selects the BatteryVoltage series"""
        got = api_client.battery(self.campbell)
        self.assertEqual([(s.key, s.label, s.unit) for s in got],
                         [("12096__BatteryVoltage__BatteryVoltage", "BatteryVoltage", "V")])

    def test_ground_temperature(self):
        """ground_temperature() gives the series with relative_z <= 0 shallow to deep with depth labels"""
        self.assertEqual([s.label for s in api_client.ground_temperature(self.tekbox)],
                         ["-0.10 m", "-0.25 m", "-0.50 m"])
        data = synthetic({
            "a": {"timeseries_name": "GroundTemperature", "relative_z": -1.0},
            "b": {"timeseries_name": "GroundTemperature", "relative_z": 0.5},
            "c": {"timeseries_name": "GroundTemperature", "relative_z": None},
            "d": {"timeseries_name": "GroundTemperature", "relative_z": 0.0},
            "e": {"timeseries_name": "AirTemp", "relative_z": -0.5},
        })
        self.assertEqual([(s.key, s.label) for s in api_client.ground_temperature(data)],
                         [("d", "+0.00 m"), ("a", "-1.00 m")])

    def test_inclination_heading_sensor_temp(self):
        """inclination() is datatype Angle, heading() datatype Heading, sensor_temperature() SensorTemp, for both loggers"""
        self.assertEqual([s.label for s in api_client.inclination(self.tekbox)], ["PitchAngle", "RollAngle"])
        self.assertEqual([s.label for s in api_client.inclination(self.campbell)], ["XAngle", "YAngle"])
        self.assertEqual([(s.label, s.unit) for s in api_client.heading(self.tekbox)], [("CompassHeading", "deg")])
        self.assertEqual(api_client.heading(self.campbell), [])
        self.assertEqual([s.label for s in api_client.sensor_temperature(self.tekbox)], ["Inclination Sensor"])
        self.assertEqual([s.label for s in api_client.sensor_temperature(self.campbell)], ["Dual Axis Inclinometer"])

    def test_weather(self):
        """weather() selects AirTemp and RelHum with their units; an unknown name is a ValueError"""
        self.assertEqual([(s.key, s.unit) for s in api_client.weather(self.tekbox, "AirTemp")],
                         [("Weather Sensor__AirTemp", "C")])
        self.assertEqual([s.unit for s in api_client.weather(self.campbell, "RelHum")], ["%"])
        with self.assertRaises(ValueError):
            api_client.weather(self.tekbox, "Wind")

    def test_pressure_pa(self):
        """BarometricPressure in Pa is converted to kPa in raw and daily series; the source is unchanged"""
        (raw,) = api_client.weather(self.tekbox, "BarometricPressure")
        self.assertEqual(raw.unit, "kPa")
        self.assertAlmostEqual(raw.series.value[0], 101.3)
        self.assertAlmostEqual(raw.series.value[1], 101.31)
        self.assertEqual(self.tekbox["Weather Sensor__BarometricPressure"].value[0], 101300.0)
        (daily,) = api_client.weather(self.campbell, "BarometricPressure")
        self.assertAlmostEqual(daily.series.mean[0], 101.335)
        self.assertAlmostEqual(daily.series.min[0], 101.3)
        self.assertAlmostEqual(daily.series.max[0], 101.37)

    def test_pressure_units(self):
        """hPa is divided by 10, kPa kept; an unknown unit is unconverted with its unit and a WARNING"""
        for kind in ("raw", "daily"):
            for unit, factor, expected_unit in (("hPa", 0.1, "kPa"), ("kPa", 1.0, "kPa"), ("bar", 1.0, "bar")):
                data = synthetic({"p": {"timeseries_name": "BarometricPressure", "unit": unit}}, kind)
                with self.subTest(kind=kind, unit=unit):
                    if expected_unit == "kPa":
                        with self.assertNoLogs("nuki.v2.api", level="WARNING"):
                            (got,) = api_client.weather(data, "BarometricPressure")
                    else:
                        with self.assertLogs("nuki.v2.api", level="WARNING"):
                            (got,) = api_client.weather(data, "BarometricPressure")
                    values = got.series.value if kind == "raw" else got.series.mean
                    self.assertEqual(got.unit, expected_unit)
                    self.assertEqual([round(v, 6) for v in values], [round(v * factor, 6) for v in (1.0, 2.0, 3.0)])

    def test_nothing_selected(self):
        """a selection without matching columns is an empty list"""
        data = synthetic({"x": {"timeseries_name": "Other"}})
        for select in (api_client.battery, api_client.ground_temperature, api_client.inclination,
                       api_client.heading, api_client.sensor_temperature):
            self.assertEqual(select(data), [])
        self.assertEqual(api_client.weather(data, "AirTemp"), [])


# ---- thinning and the ported TSP modules ------------------------------------------------------

class ThinTest(unittest.TestCase):
    def test_bucket_min_max(self):
        """thin_series keeps the min and the max of every bucket at their own times (a one-sample spike survives)"""
        n = 10_000
        t = np.datetime64("2026-01-01T00:00", "ms") + np.arange(n) * np.timedelta64(60_000, "ms")
        v = np.sin(np.arange(n) / 50.0)
        v[4321] = 9.0
        start, end = t[0], t[-1]
        kt, kv, thinned, bin_s = rawseries.thin_series(t, v, start, end, max_points=200)
        self.assertTrue(thinned)
        self.assertLessEqual(len(kt), 200)
        self.assertIn(9.0, kv.tolist())
        width = (end - start) / np.timedelta64(1, "ms") / 100
        self.assertAlmostEqual(bin_s, width / 1000)
        bins = np.minimum(((t - start) / np.timedelta64(1, "ms") / width).astype(int), 99)
        kept = set(zip(kt.tolist(), kv.tolist()))
        for b in range(100):
            sel = bins == b
            i_min, i_max = np.argmin(v[sel]), np.argmax(v[sel])
            self.assertIn((t[sel][i_min].tolist(), v[sel][i_min]), kept)
            self.assertIn((t[sel][i_max].tolist(), v[sel][i_max]), kept)
        self.assertTrue((np.diff(kt) >= np.timedelta64(0, "ms")).all())

    def test_sparse_unchanged(self):
        """thin_series returns few points unchanged, with a NaN break at a long gap and NaN values dropped"""
        t = np.array(["2026-01-01T00:00", "2026-01-01T01:00", "2026-01-01T02:00", "2026-01-02T02:00",
                      "2026-01-02T03:00"], dtype="datetime64[ms]")
        v = np.array([1.0, np.nan, 3.0, 4.0, 5.0])
        kt, kv, thinned, bin_s = rawseries.thin_series(t, v, t[0], t[-1])
        self.assertEqual((thinned, bin_s), (False, None))
        self.assertEqual(len(kt), 5)            # 4 points and one break
        self.assertTrue(np.isnan(kv[2]))
        self.assertEqual(kv[[0, 1, 3, 4]].tolist(), [1.0, 3.0, 4.0, 5.0])

    def test_thin_window(self):
        """thin_series keeps only the window start..end (inclusive)"""
        t = np.array(["2026-01-01T00:00", "2026-01-01T01:00", "2026-01-01T02:00"], dtype="datetime64[ms]")
        kt, kv, _thinned, _b = rawseries.thin_series(t, [1.0, 2.0, 3.0], "2026-01-01T01:00Z", "2026-01-01T02:00:00")
        self.assertEqual(kv.tolist(), [2.0, 3.0])


class PortedTspTest(ClientTestCase):
    def test_rawseries_thin(self):
        """rawseries.parse_raw and thin read the client's raw payloads (3 ground sensors, not thinned)"""
        result = self.client.raw(TEKBOX_M6, "2026-09-01", "2026-09-05", now=NOW)
        raw = rawseries.parse_raw(result.payloads)
        self.assertEqual(raw.depths.tolist(), [0.1, 0.25, 0.5])
        thinned = rawseries.thin(raw, "2026-09-01T00:00:00Z", "2026-09-05T00:00:00Z")
        self.assertEqual([s["thinned"] for s in thinned.series], [False] * 3)
        self.assertEqual(len(thinned.series[0]["mean"]), 33)
        dense = rawseries.thin(raw, "2026-09-01T00:00:00Z", "2026-09-05T00:00:00Z", max_points=10)
        self.assertTrue(all(s["thinned"] for s in dense.series))
        self.assertIsNotNone(dense.bin_seconds)

    def test_trumpet_parse_daily(self):
        """trumpet.parse_daily reads the client's daily payloads (depths, counts per day)"""
        result = self.client.daily(TEKBOX_M6, "2026-09-01", "2026-09-05", now=NOW)
        daily = trumpet.parse_daily(result.payloads)
        self.assertEqual(daily.depths.tolist(), [0.1, 0.25, 0.5])
        self.assertEqual(daily.count[0].tolist(), [8, 8, 8, 8, 1])
        self.assertEqual(trumpet.years(daily), [2026])


if __name__ == "__main__":
    unittest.main()
