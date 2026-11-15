"""Unit tests of app/db_connector.py against the stub monitoring_db API (tests/fake_api.py):
base URL, retries and error mapping, metadata caches, the mast catalogue, chunked v2 raw
data, the selection helpers and the date normalisation. The fixture data is described in
tests/fixtures/make_fixtures.py. Each test's docstring is its name in the test output ("unit: db_connector: <docstring>")."""
import asyncio
import logging
import os
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import pandas as pd
import pydantic

from support import ApiTestCase, closed_port, config, db_connector, fake_api, silent_port

DAY = "2026-10-01"
DAY_START = "2026-10-01T00:00:00Z"
DAY_END = "2026-10-01T23:59:59.999000Z"

TEKBOX_M6 = "deveui:feedbeefcafe0002"          # device 10, 2026-09-01..05, offset 0
CAMPBELL_M6 = "serial:SIS2024-01:12096"        # device 11, 2026-10-01..05, offset 1
TEKBOX_M9 = "deveui:feedbeefcafe0001"          # device 20, 2024-10-14..18, offset 2
STEP = timedelta(hours=3)


def utc(s):
    return pd.Timestamp(s, tz="UTC")


def synthetic(columns, n=3):
    """LoggerData without HTTP: columns is {key: details}; values 1, 2, 3 ... per column."""
    index = pd.date_range("2026-10-01", periods=n, freq="3h", tz="UTC", name="timestamp")
    frame = pd.DataFrame({k: [float(i + 1) for i in range(n)] for k in columns}, index=index)
    return db_connector.LoggerData(frame=frame, columns={k: dict(v, key=k) for k, v in columns.items()})


# ---- configuration ----------------------------------------------------------------------------

class ConfigTest(unittest.TestCase):
    def test_defaults(self):
        """the new settings default to 2 in parallel, 1-year chunks, the Nuki pattern and the root URL"""
        fields = config.ConfigSettings.model_fields
        self.assertEqual(fields["backend_api_max_parallel"].default, 2)
        self.assertEqual(fields["backend_api_raw_chunk_years"].default, 1)
        self.assertEqual(fields["nuki_station_pattern"].default, r"^(SIS2024-0[1-5]|NUKI202[0-9]-.+)$")
        self.assertEqual(fields["backend_api_base_url"].default, "http://host.docker.internal:8099")

    def test_env_values(self):
        """BACKEND_API_MAX_PARALLEL, BACKEND_API_RAW_CHUNK_YEARS and NUKI_STATION_PATTERN are read from env"""
        env = {"BACKEND_API_MAX_PARALLEL": "3", "BACKEND_API_RAW_CHUNK_YEARS": "2",
               "NUKI_STATION_PATTERN": "QAA.*"}
        with mock.patch.dict(os.environ, env):
            s = config.ConfigSettings()
        self.assertEqual((s.backend_api_max_parallel, s.backend_api_raw_chunk_years), (3, 2))
        self.assertTrue(s.station_regex.fullmatch("QAA2017-01"))

    def test_invalid_pattern(self):
        """an invalid NUKI_STATION_PATTERN fails at config load with a message naming the variable"""
        with mock.patch.dict(os.environ, {"NUKI_STATION_PATTERN": "(SIS2024"}):
            with self.assertRaisesRegex(pydantic.ValidationError, "NUKI_STATION_PATTERN is not a valid regular expression"):
                config.ConfigSettings()

    def test_non_positive_ints(self):
        """BACKEND_API_MAX_PARALLEL=0 or BACKEND_API_RAW_CHUNK_YEARS=0 fails at config load"""
        for name in ("BACKEND_API_MAX_PARALLEL", "BACKEND_API_RAW_CHUNK_YEARS"):
            with self.subTest(name=name), mock.patch.dict(os.environ, {name: "0"}):
                with self.assertRaises(pydantic.ValidationError):
                    config.ConfigSettings()


# ---- base URL and transport -------------------------------------------------------------------

class BaseUrlTest(ApiTestCase):
    def test_root_form(self):
        """the root URL form and a trailing slash are used as they are, without a warning"""
        for url in (self.root_url, self.root_url + "/"):
            self.set_settings(backend_api_base_url=url)
            with self.assertNoLogs("db_connector", level="WARNING"):
                self.assertEqual(db_connector._root_url(), self.root_url)

    def test_suffix_stripped(self):
        """a trailing /api/v1 or /api/v2 (or /api/v1/) is stripped, with one WARNING per value"""
        for url in (self.root_url + "/api/v1", self.root_url + "/api/v2", self.root_url + "/api/v1/"):
            self.set_settings(backend_api_base_url=url)
            with self.assertLogs("db_connector", level="WARNING") as logs:
                self.assertEqual(db_connector._root_url(), self.root_url)
            self.assertEqual(len(logs.records), 1)
            self.assertIn("API version", logs.output[0])
            with self.assertNoLogs("db_connector", level="WARNING"):
                self.assertEqual(db_connector._root_url(), self.root_url)

    def test_v2_path(self):
        """with the production form .../mdb/api/v1 the raw data comes from <root>/api/v2/measurements/raw"""
        self.set_settings(backend_api_base_url=self.v1_url)
        db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-01")
        self.assertEqual([p for p, _ in self.server.requests], ["/mdb/api/v2/measurements/raw"])


class RetryTest(ApiTestCase):
    def test_retry_config(self):
        """the session retries connection errors only (connect=BACKEND_API_MAX_RETRIES, no read, status or other)"""
        self.set_settings(backend_api_max_retries=3)
        with db_connector._build_session() as session:
            retry = session.get_adapter("http://x/").max_retries
        self.assertEqual(retry.connect, 3)
        self.assertIs(retry.read, False)
        self.assertEqual((retry.status, retry.other), (0, 0))
        self.assertFalse(retry.status_forcelist)

    def test_504_v2(self):
        """504 on /raw raises QueryTimeout after one request, also with retries configured"""
        self.set_settings(backend_api_max_retries=2)
        self.server.force = fake_api.QUERY_TIMEOUT
        with self.assertRaises(db_connector.QueryTimeout):
            db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-05")
        self.assertEqual(len(self.server.requests), 1)

    def test_read_timeout(self):
        """an API that does not answer within the timeout raises QueryTimeout after one connection (not retried)"""
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
        self.set_settings(backend_api_base_url=f"http://127.0.0.1:{port}/mdb",
                          backend_api_timeout_seconds=0.5, backend_api_max_retries=2)
        with self.assertRaises(db_connector.QueryTimeout):
            db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-05")
        time.sleep(0.2)
        self.assertEqual(len(connections), 1)

    def test_connection_refused(self):
        """an unreachable API (connection refused, retried) raises RuntimeError (transport error)"""
        self.set_settings(backend_api_base_url=f"http://127.0.0.1:{closed_port()}/mdb",
                          backend_api_max_retries=2)
        with self.assertRaisesRegex(RuntimeError, "transport error"):
            db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-05")

    def test_accept_gzip(self):
        """the /raw request sends Accept-Encoding with gzip"""
        db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-01")
        self.assertIn("gzip", self.server.headers[-1].get("accept-encoding", ""))


# ---- metadata caches and catalogue ------------------------------------------------------------

class MetadataCacheTest(ApiTestCase):
    def test_v1_cached(self):
        """v1 metadata is fetched once and then served from the cache"""
        first = db_connector.metadata_v1()
        second = db_connector.metadata_v1()
        self.assertIs(first, second)
        self.assertEqual([p for p, _ in self.server.requests], ["/mdb/api/v1/metadata"])

    def test_v1_expires(self):
        """the v1 metadata cache expires after 10 minutes"""
        now = [1000.0]
        with mock.patch.object(db_connector, "_monotonic", lambda: now[0]):
            db_connector.metadata_v1()
            now[0] += 599
            db_connector.metadata_v1()
            now[0] += 2
            db_connector.metadata_v1()
        self.assertEqual(len(self.api_requests("v1/metadata")), 2)

    def test_failure_not_cached(self):
        """a failed metadata request is not cached: the next call asks again"""
        self.server.force = (500, {"error": {"code": "INTERNAL_ERROR", "message": "x", "details": {}}})
        with self.assertRaises(RuntimeError):
            db_connector.metadata_v1()
        self.server.force = None
        self.assertIn("stations", db_connector.metadata_v1())
        self.assertEqual(len(self.api_requests("v1/metadata")), 2)

    def test_v2_per_station(self):
        """v2 metadata is requested with station=<identifier> and cached per station"""
        a = db_connector.metadata_v2("SIS2024-01")
        db_connector.metadata_v2("SIS2024-01")
        db_connector.metadata_v2("SIS2024-03")
        self.assertEqual(self.api_requests("v2/metadata"), [{"station": "SIS2024-01"}, {"station": "SIS2024-03"}])
        self.assertEqual([s["identifier"] for s in a["stations"]], ["SIS2024-01"])

    def test_catalogue_cached(self):
        """a second mast_catalogue() call makes no request"""
        db_connector.mast_catalogue()
        n = len(self.server.requests)
        db_connector.mast_catalogue()
        self.assertEqual(len(self.server.requests), n)
        self.assertEqual(n, 1 + 4)   # v1 metadata + v2 metadata of the 4 included stations


class CatalogueTest(ApiTestCase):
    def test_masts(self):
        """the catalogue lists Mast 6, 9 and 13 (numeric order), not QAA2017-01 or SIS2024-07"""
        masts = db_connector.mast_catalogue()
        self.assertEqual([m["name"] for m in masts], ["Mast 6", "Mast 9", "Mast 13"])
        idents = [i for m in masts for i in m["station_identifiers"]]
        self.assertNotIn("QAA2017-01", idents)
        self.assertNotIn("SIS2024-07", idents)
        self.assertEqual({s["station"] for s in self.api_requests("v2/metadata")},
                         {"SIS2024-01", "SIS2024-03", "NUKI2026-M9", "SIS2024-05"})

    def test_mast6(self):
        """Mast 6 has the Tekbox (deveui) and the Campbell (station and serial), by first measurement"""
        mast = db_connector.mast_catalogue()[0]
        self.assertEqual(mast["station_identifiers"], ["SIS2024-01"])
        self.assertEqual(mast["loggers"], [
            {"value": TEKBOX_M6, "label": "Tekbox_TBSL1_3 (feedbeefcafe0002), 2026-09-01 to 2026-09-05",
             "station": "SIS2024-01", "first": "2026-09-01T00:00:00Z", "last": "2026-09-05T00:00:00Z"},
            {"value": CAMPBELL_M6, "label": "CR300_M6 (12096), 2026-10-01 to 2026-10-05",
             "station": "SIS2024-01", "first": "2026-10-01T00:00:00Z", "last": "2026-10-05T00:00:00Z"},
        ])

    def test_mast9_two_stations(self):
        """Mast 9 groups SIS2024-03 (Tekbox) and NUKI2026-M9 (Campbell) by station name"""
        mast = db_connector.mast_catalogue()[1]
        self.assertEqual(sorted(mast["station_identifiers"]), ["NUKI2026-M9", "SIS2024-03"])
        self.assertEqual([lg["value"] for lg in mast["loggers"]], [TEKBOX_M9, "serial:NUKI2026-M9:12215"])
        self.assertEqual([lg["station"] for lg in mast["loggers"]], ["SIS2024-03", "NUKI2026-M9"])

    def test_mast13_without_tekbox(self):
        """Mast 13 has only CR300_M13: the Tekbox without measurements is left out"""
        mast = db_connector.mast_catalogue()[2]
        self.assertEqual([lg["value"] for lg in mast["loggers"]], ["serial:SIS2024-05:12216"])
        self.assertTrue(mast["loggers"][0]["label"].startswith("CR300_M13 (12216), "))

    def test_custom_pattern(self):
        """a custom NUKI_STATION_PATTERN applies (fullmatch); a mast name without the prefix is kept"""
        self.set_settings(nuki_station_pattern=r"QAA\d{4}-01")
        masts = db_connector.mast_catalogue()
        self.assertEqual([m["name"] for m in masts], ["Qaanaaq borehole 1"])
        self.assertEqual([lg["value"] for lg in masts[0]["loggers"]], ["serial:QAA2017-01:A522C6"])
        db_connector.clear_caches()
        self.set_settings(nuki_station_pattern="SIS2024-0")
        self.assertEqual(db_connector.mast_catalogue(), [])

    def test_mast_without_data(self):
        """a mast whose loggers have no measurements is left out"""
        self.set_settings(nuki_station_pattern="SIS2024-05")
        self.server.fixtures["measurements"] = {k: v for k, v in self.server.fixtures["measurements"].items()
                                                if not str(k).startswith("41")}
        self.assertEqual(db_connector.mast_catalogue(), [])

    def test_logger_without_ids(self):
        """a device with empty serial and deveui is skipped; a null serial falls back to the deveui"""
        entry = db_connector._logger_entry("X", {"serial": "", "deveui": None, "display_name": "L",
                                                 "first_measurement_time": "2026-01-01T00:00:00Z",
                                                 "last_measurement_time": "2026-01-02T00:00:00Z"})
        self.assertIsNone(entry)
        entry = db_connector._logger_entry("X", {"serial": None, "deveui": "abc", "display_name": "L",
                                                 "first_measurement_time": "2026-01-01T00:00:00Z",
                                                 "last_measurement_time": "2026-01-02T00:00:00Z"})
        self.assertEqual(entry["value"], "deveui:abc")

    def test_default_logger(self):
        """default_logger is the logger with the latest last measurement"""
        masts = db_connector.mast_catalogue()
        self.assertEqual([db_connector.default_logger(m)["value"] for m in masts],
                         [CAMPBELL_M6, "serial:NUKI2026-M9:12215", "serial:SIS2024-05:12216"])
        self.assertIsNone(db_connector.default_logger({"loggers": []}))

    def test_async(self):
        """mast_catalogue_async returns the same catalogue"""
        masts = asyncio.run(db_connector.mast_catalogue_async())
        self.assertEqual([m["name"] for m in masts], ["Mast 6", "Mast 9", "Mast 13"])


# ---- raw data ---------------------------------------------------------------------------------

class LoggerDataTest(ApiTestCase):
    def test_tekbox(self):
        """a Tekbox (deveui) gives one float column per series with the fixture values, UTC index"""
        data = db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-05")
        self.assertEqual(self.api_requests("measurements/raw"),
                         [{"deveui": "feedbeefcafe0002", "start": "2026-09-01T00:00:00Z",
                           "end": "2026-09-05T23:59:59Z"}])
        frame = data.frame
        self.assertEqual(str(frame.index.tz), "UTC")
        self.assertTrue(frame.index.is_monotonic_increasing)
        self.assertEqual(len(frame), 33)
        self.assertEqual(set(frame.columns), set(data.columns))
        self.assertTrue(all(dtype == float for dtype in frame.dtypes))
        k = 10   # 2026-09-02 06:00
        row = frame.loc[utc("2026-09-01") + k * STEP]
        self.assertAlmostEqual(row["BatteryVoltage__BatteryVoltage"], 3.7 - 0.001 * k)
        self.assertAlmostEqual(row["GroundTemp02__GroundTemperature"], -0.2 - 0.002 * k)
        self.assertAlmostEqual(row["Weather Sensor__AirTemp"], 2.0 - 0.05 * k)
        detail = data.columns["GroundTemp02__GroundTemperature"]
        self.assertEqual((detail["timeseries_name"], detail["sensor_name"], detail["relative_z"], detail["unit"]),
                         ("GroundTemperature", "GroundTemp02", -0.25, "C"))

    def test_campbell(self):
        """a Campbell (station + serial) is fetched with both; keys carry the serial prefix, gaps are NaN"""
        data = db_connector.get_logger_data(CAMPBELL_M6, "2026-10-01", "2026-10-02")
        self.assertEqual([(p["station"], p["serial"]) for p in self.api_requests("measurements/raw")],
                         [("SIS2024-01", "12096")])
        self.assertNotIn("deveui", self.api_requests("measurements/raw")[0])
        frame = data.frame
        self.assertTrue(all(k.startswith("12096__") for k in frame.columns))
        self.assertAlmostEqual(frame.loc[utc("2026-10-01T03:00"), "12096__BatteryVoltage__BatteryVoltage"], 13.0 - 0.002)
        self.assertAlmostEqual(frame.loc[utc("2026-10-01T03:00"), "12096__GroundTemp01__GroundTemperature"],
                               -0.1 - 1.0 - 0.002)
        self.assertTrue(pd.isna(frame.loc[utc("2026-10-01T03:00"), "12096__Dual Axis Inclinometer__XAngle"]))
        self.assertAlmostEqual(frame.loc[utc("2026-10-01T06:00"), "12096__Dual Axis Inclinometer__XAngle"], 0.452)

    def test_datetimes(self):
        """datetimes are passed as UTC seconds (naive taken as UTC)"""
        db_connector.get_logger_data(TEKBOX_M6, datetime(2026, 9, 1, 6), "2026-09-01T12:00:00+02:00")
        params = self.api_requests("measurements/raw")[0]
        self.assertEqual((params["start"], params["end"]), ("2026-09-01T06:00:00Z", "2026-09-01T10:00:00Z"))

    def test_chunks(self):
        """a 2.5-year range gives 3 requests, contiguous without overlap, results in time order"""
        active, peak, lock = [0], [0], threading.Lock()
        real = db_connector._api_get
        delays = {"2022-10-16T00:00:00Z": 0.4, "2023-10-16T00:00:00Z": 0.2}

        def slow_get(path, params):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            try:
                time.sleep(delays.get(params.get("start"), 0.0))   # the earlier chunk answers last
                return real(path, params)
            finally:
                with lock:
                    active[0] -= 1

        with mock.patch.object(db_connector, "_api_get", slow_get):
            data = db_connector.get_logger_data(TEKBOX_M9, "2022-10-16", "2025-04-15")
        bounds = sorted((p["start"], p["end"]) for p in self.api_requests("measurements/raw"))
        self.assertEqual(bounds, [("2022-10-16T00:00:00Z", "2023-10-15T23:59:59Z"),
                                  ("2023-10-16T00:00:00Z", "2024-10-15T23:59:59Z"),
                                  ("2024-10-16T00:00:00Z", "2025-04-15T23:59:59Z")])
        for (_, end), (start, _) in zip(bounds, bounds[1:]):
            self.assertEqual(datetime.fromisoformat(start.replace("Z", "+00:00"))
                             - datetime.fromisoformat(end.replace("Z", "+00:00")), timedelta(seconds=1))
        self.assertEqual(peak[0], 2)
        frame = data.frame
        self.assertTrue(frame.index.is_monotonic_increasing)
        self.assertEqual(len(frame), 33)    # 2024-10-14..18, split over chunks 2 and 3
        self.assertEqual(frame.index[0], utc("2024-10-14"))
        self.assertEqual(frame.index[-1], utc("2024-10-18"))
        k = 20   # 2024-10-16 12:00, in chunk 3
        self.assertAlmostEqual(frame.loc[utc("2024-10-14") + k * STEP, "BatteryVoltage__BatteryVoltage"],
                               3.7 - 0.001 * k)

    def test_max_parallel_1(self):
        """with BACKEND_API_MAX_PARALLEL=1 the chunks are fetched one at a time"""
        self.set_settings(backend_api_max_parallel=1)
        active, peak, lock = [0], [0], threading.Lock()
        real = db_connector._api_get

        def slow_get(path, params):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            try:
                time.sleep(0.1)
                return real(path, params)
            finally:
                with lock:
                    active[0] -= 1

        with mock.patch.object(db_connector, "_api_get", slow_get):
            db_connector.get_logger_data(TEKBOX_M9, "2022-10-16", "2025-04-15")
        self.assertEqual((len(self.api_requests("measurements/raw")), peak[0]), (3, 1))

    def test_chunk_years(self):
        """BACKEND_API_RAW_CHUNK_YEARS=3 fetches the 2.5-year range in one request"""
        self.set_settings(backend_api_raw_chunk_years=3)
        db_connector.get_logger_data(TEKBOX_M9, "2022-10-16", "2025-04-15")
        self.assertEqual(len(self.api_requests("measurements/raw")), 1)

    def test_chunk_error_cancels(self):
        """the first failing chunk raises its error and the queued chunks are not all sent"""
        self.set_settings(backend_api_max_parallel=1)
        self.server.force = (500, {"error": {"code": "INTERNAL_ERROR", "message": "x", "details": {}}})
        with self.assertRaises(RuntimeError):
            db_connector.get_logger_data(TEKBOX_M9, "2022-10-16", "2025-04-15")
        time.sleep(0.2)
        self.assertLess(len(self.server.requests), 3)

    def test_chunks_now(self):
        """chunks() ends at now and is empty for a period in the future"""
        now = datetime(2026, 11, 15, 12, 0, 0, 500, tzinfo=timezone.utc)
        first = datetime(2026, 11, 1, tzinfo=timezone.utc)
        self.assertEqual(db_connector.chunks(first, datetime(2026, 12, 1, tzinfo=timezone.utc), 1, now),
                         [("2026-11-01T00:00:00Z", "2026-11-15T12:00:00Z")])
        self.assertEqual(db_connector.chunks(datetime(2027, 1, 1, tzinfo=timezone.utc),
                                             datetime(2027, 2, 1, tzinfo=timezone.utc), 1, now), [])

    def test_empty(self):
        """no rows in the range raises EmptyQuerySet"""
        with self.assertRaises(db_connector.EmptyQuerySet):
            db_connector.get_logger_data(TEKBOX_M6, "2025-01-01", "2025-01-02")

    def test_future_range(self):
        """a range entirely in the future raises EmptyQuerySet without a request"""
        with self.assertRaises(db_connector.EmptyQuerySet):
            db_connector.get_logger_data(TEKBOX_M6, "2099-01-01", "2099-01-02")
        self.assertEqual(self.server.requests, [])

    def test_invalid_logger(self):
        """an invalid logger value raises ValueError without a request"""
        for value in ("", "foo:1", "serial:12096", "deveui:"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                db_connector.get_logger_data(value, "2026-09-01", "2026-09-05")
        self.assertEqual(self.server.requests, [])

    def test_404(self):
        """404 (unknown deveui or serial) raises RecordingDeviceNotFound"""
        for value in ("deveui:0000000000000000", "serial:SIS2024-01:99999"):
            with self.subTest(value=value), self.assertRaises(db_connector.RecordingDeviceNotFound):
                db_connector.get_logger_data(value, "2026-09-01", "2026-09-05")

    def test_400_message(self):
        """400 raises ValueError with the API's error message"""
        self.server.force = (400, {"error": {"code": "INVALID_QUERY", "message": "bad start", "details": {}}})
        with self.assertRaisesRegex(ValueError, "^bad start$"):
            db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-05")

    def test_400_no_json(self):
        """400 without a JSON body raises ValueError 'Invalid backend API request'"""
        self.server.force = (400, b"<html>bad request</html>")
        with self.assertRaisesRegex(ValueError, "^Invalid backend API request$"):
            db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-05")

    def test_500(self):
        """500 on /raw raises RuntimeError (internal error)"""
        self.server.force = (500, {"error": {"code": "INTERNAL_ERROR", "message": "x", "details": {}}})
        with self.assertRaisesRegex(RuntimeError, r"internal error \(500\)"):
            db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-05")

    def test_invalid_json(self):
        """200 with a body that is not JSON raises RuntimeError"""
        self.server.force = (200, b"not json")
        with self.assertRaisesRegex(RuntimeError, "invalid JSON"):
            db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-05")

    def test_invalid_shape(self):
        """200 with JSON of the wrong shape raises RuntimeError"""
        for body in ({"time": ["2026-09-01T00:00:00Z"], "columns": [], "values": [[1.0]]},
                     {"time": ["2026-09-01T00:00:00Z"], "columns": [{"key": "a"}], "values": [[1.0, 2.0]]},
                     {"rows": []}, [1, 2]):
            self.server.force = (200, body if isinstance(body, dict) else b"[1, 2]")
            with self.subTest(body=body), self.assertRaises(RuntimeError):
                db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-05")

    def test_async(self):
        """get_logger_data_async returns the same data; the wrappers are not thread-sensitive"""
        data = asyncio.run(db_connector.get_logger_data_async(TEKBOX_M6, "2026-09-01", "2026-09-01"))
        self.assertEqual(len(data.frame), 8)
        for name in ("mast_catalogue_async", "get_logger_data_async"):
            self.assertIs(getattr(db_connector, name)._thread_sensitive, False, name)


# ---- selections -------------------------------------------------------------------------------

class SelectionTest(ApiTestCase):
    def setUp(self):
        super().setUp()
        self.tekbox = db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-01")
        self.campbell = db_connector.get_logger_data(CAMPBELL_M6, "2026-10-01", "2026-10-01")

    def test_battery(self):
        """battery() selects the BatteryVoltage series"""
        frame, labels = db_connector.battery(self.campbell)
        self.assertEqual(list(frame.columns), ["12096__BatteryVoltage__BatteryVoltage"])
        self.assertEqual(labels, ["BatteryVoltage"])
        self.assertAlmostEqual(frame.iloc[0, 0], 13.0)

    def test_ground_temperature(self):
        """ground_temperature() gives the GroundTemperature series shallow to deep with depth labels"""
        frame, labels = db_connector.ground_temperature(self.tekbox)
        self.assertEqual(labels, ["-0.10 m", "-0.25 m", "-0.50 m"])
        self.assertEqual(list(frame.columns), ["GroundTemp01__GroundTemperature", "GroundTemp02__GroundTemperature",
                                               "GroundTemp03__GroundTemperature"])

    def test_ground_temperature_depth_filter(self):
        """ground_temperature() leaves out series above ground or without relative_z, sorts by depth"""
        data = synthetic({
            "a": {"timeseries_name": "GroundTemperature", "relative_z": -1.0},
            "b": {"timeseries_name": "GroundTemperature", "relative_z": 0.5},
            "c": {"timeseries_name": "GroundTemperature", "relative_z": None},
            "d": {"timeseries_name": "GroundTemperature", "relative_z": 0.0},
            "e": {"timeseries_name": "AirTemp", "relative_z": -0.5},
        })
        frame, labels = db_connector.ground_temperature(data)
        self.assertEqual((list(frame.columns), labels), (["d", "a"], ["+0.00 m", "-1.00 m"]))

    def test_weather(self):
        """weather() selects AirTemp and RelHum with their units"""
        frame, labels, unit = db_connector.weather(self.tekbox, "AirTemp")
        self.assertEqual((list(frame.columns), labels, unit), (["Weather Sensor__AirTemp"], ["AirTemp"], "C"))
        frame, labels, unit = db_connector.weather(self.campbell, "RelHum")
        self.assertEqual((list(frame.columns), unit), (["12096__Weather Sensor__RelHum"], "%"))

    def test_pressure_pa(self):
        """BarometricPressure in Pa is converted to kPa (about 101.3)"""
        frame, labels, unit = db_connector.weather(self.tekbox, "BarometricPressure")
        self.assertEqual((labels, unit), (["BarometricPressure"], "kPa"))
        self.assertAlmostEqual(frame.iloc[0, 0], 101.3)
        self.assertAlmostEqual(frame.iloc[1, 0], 101.31)
        # the LoggerData itself is unchanged
        self.assertAlmostEqual(self.tekbox.frame["Weather Sensor__BarometricPressure"].iloc[0], 101300.0)

    def test_pressure_units(self):
        """hPa is divided by 10, kPa kept; an unknown unit is unconverted with its unit and a WARNING"""
        for unit, factor, expected_unit in (("hPa", 0.1, "kPa"), ("kPa", 1.0, "kPa"), ("bar", 1.0, "bar")):
            data = synthetic({"p": {"timeseries_name": "BarometricPressure", "unit": unit}})
            with self.subTest(unit=unit):
                if expected_unit == "kPa":
                    frame, _labels, got = db_connector.weather(data, "BarometricPressure")
                else:
                    with self.assertLogs("db_connector", level="WARNING"):
                        frame, _labels, got = db_connector.weather(data, "BarometricPressure")
                self.assertEqual(got, expected_unit)
                self.assertEqual([round(v, 6) for v in frame["p"]], [round(v * factor, 6) for v in (1.0, 2.0, 3.0)])

    def test_weather_missing(self):
        """weather() raises EmptyQuerySet when the series is missing and ValueError for an unknown name"""
        data = synthetic({"x": {"timeseries_name": "BatteryVoltage"}})
        with self.assertRaises(db_connector.EmptyQuerySet):
            db_connector.weather(data, "AirTemp")
        with self.assertRaises(ValueError):
            db_connector.weather(data, "WindSpeed")

    def test_inclination_tekbox(self):
        """inclination() gives Roll/Pitch of the Tekbox, without CompassHeading and SensorTemp"""
        frame, labels = db_connector.inclination(self.tekbox)
        self.assertEqual(sorted(labels), ["PitchAngle", "RollAngle"])
        self.assertEqual(len(frame.columns), 2)

    def test_inclination_campbell(self):
        """inclination() gives XAngle/YAngle of the Campbell, without SensorTemp"""
        frame, labels = db_connector.inclination(self.campbell)
        self.assertEqual(sorted(labels), ["XAngle", "YAngle"])
        self.assertTrue(all("Dual Axis Inclinometer" in k for k in frame.columns))

    def test_nothing_selected(self):
        """battery(), ground_temperature() and inclination() raise EmptyQuerySet when nothing matches"""
        data = synthetic({"x": {"timeseries_name": "AirTemp", "datatype": "Temperature"}})
        for helper in (db_connector.battery, db_connector.ground_temperature, db_connector.inclination):
            with self.subTest(helper=helper.__name__), self.assertRaises(db_connector.EmptyQuerySet):
                helper(data)


# ---- dates ------------------------------------------------------------------------------------

class DateTest(unittest.TestCase):
    def test_date_only(self):
        """a YYYY-MM-DD range covers the whole end day: 00:00:00Z to 23:59:59.999Z"""
        self.assertEqual(db_connector._normalize_range_for_api(DAY, "2026-10-31"),
                         (DAY_START, "2026-10-31T23:59:59.999000Z"))

    def test_naive_datetime(self):
        """a datetime without a time zone is taken as UTC"""
        self.assertEqual(db_connector._normalize_datetime_for_api("2026-10-01 12:30"), "2026-10-01T12:30:00Z")

    def test_offset_datetime(self):
        """a datetime with an offset is converted to UTC"""
        self.assertEqual(db_connector._normalize_datetime_for_api("2026-10-01T02:00:00+02:00"), DAY_START)

    def test_empty(self):
        """None and an empty string normalise to None"""
        self.assertIsNone(db_connector._normalize_datetime_for_api(None))
        self.assertIsNone(db_connector._normalize_datetime_for_api(""))

    def test_default_range(self):
        """an open range defaults to the last 30 days up to now"""
        before = datetime.now(timezone.utc)
        start, end = db_connector._normalize_range_for_api(None, None)
        after = datetime.now(timezone.utc)
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        self.assertTrue(before <= end_dt <= after, end)
        self.assertTrue(before - timedelta(days=30) <= start_dt <= after - timedelta(days=30), start)


if __name__ == "__main__":
    unittest.main()
