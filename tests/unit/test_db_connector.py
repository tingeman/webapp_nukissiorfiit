"""Unit tests of app/db_connector.py against the stub monitoring_db API (tests/fake_api.py):
request building, date normalisation, error mapping, JSON -> DataFrame. Each test's
docstring is its name in the test output ("unit: db_connector: <docstring>")."""
import asyncio
import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from support import DEVEUI, ApiTestCase, closed_port, db_connector, silent_port

DAY = "2026-10-01"
DAY_START = "2026-10-01T00:00:00Z"
DAY_END = "2026-10-01T23:59:59.999000Z"


class RequestTest(ApiTestCase):
    def test_sensor_request(self):
        """get_sensor_data sends GET <base>/measurements with deveui, sensor_name, start, end"""
        db_connector.get_sensor_data(DEVEUI, "BatteryVoltage", DAY, DAY)
        self.assertEqual(
            self.server.requests,
            [("/mdb/api/v1/measurements",
              {"deveui": DEVEUI, "sensor_name": "BatteryVoltage", "start": DAY_START, "end": DAY_END})],
        )

    def test_trailing_slash(self):
        """a base URL with a trailing slash gives the same request path (no double slash)"""
        self.set_settings(backend_api_base_url=self.base_url + "/")
        db_connector.get_sensor_data(DEVEUI, "BatteryVoltage", DAY, DAY)
        self.assertEqual([p for p, _ in self.server.requests], ["/mdb/api/v1/measurements"])

    def test_ground_temp_request(self):
        """get_ground_temp reads metadata, then asks for the groundtemp sensors with sensor_name__in"""
        db_connector.get_ground_temp(DEVEUI, DAY, DAY)
        self.assertEqual([p for p, _ in self.server.requests],
                         ["/mdb/api/v1/metadata", "/mdb/api/v1/measurements"])
        self.assertEqual(self.api_requests("metadata"), [{}])
        self.assertEqual(
            self.api_requests("measurements"),
            [{"deveui": DEVEUI, "sensor_name__in": "GroundTemp01,GroundTemp02,GroundTemp03",
              "start": DAY_START, "end": DAY_END}],
        )


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


class ErrorTest(ApiTestCase):
    def test_404(self):
        """404 (unknown deveui) raises RecordingDeviceNotFound"""
        with self.assertRaises(db_connector.RecordingDeviceNotFound):
            db_connector.get_sensor_data("0000000000000000", "BatteryVoltage", DAY, DAY)

    def test_400_message(self):
        """400 raises ValueError with the API's error message"""
        self.server.force = (400, {"error": {"code": "INVALID_QUERY", "message": "bad start", "details": {}}})
        with self.assertRaisesRegex(ValueError, "^bad start$"):
            db_connector.get_sensor_data(DEVEUI, "BatteryVoltage", DAY, DAY)

    def test_400_no_json(self):
        """400 without a JSON body raises ValueError 'Invalid backend API request'"""
        self.server.force = (400, b"<html>bad request</html>")
        with self.assertRaisesRegex(ValueError, "^Invalid backend API request$"):
            db_connector.get_sensor_data(DEVEUI, "BatteryVoltage", DAY, DAY)

    def test_504_retried(self):
        """504 is retried (backend_api_max_retries), then raises QueryTimeout"""
        self.set_settings(backend_api_max_retries=2)
        self.server.force = (504, {"error": {"code": "QUERY_TIMEOUT", "message": "timeout", "details": {}}})
        with self.assertRaises(db_connector.QueryTimeout):
            db_connector.get_sensor_data(DEVEUI, "BatteryVoltage", DAY, DAY)
        self.assertEqual(len(self.server.requests), 3)

    def test_500(self):
        """500 raises RuntimeError (internal error)"""
        self.server.force = (500, {"error": {"code": "INTERNAL_ERROR", "message": "x", "details": {}}})
        with self.assertRaisesRegex(RuntimeError, r"internal error \(500\)"):
            db_connector.get_sensor_data(DEVEUI, "BatteryVoltage", DAY, DAY)

    def test_invalid_json(self):
        """200 with a body that is not JSON raises RuntimeError"""
        self.server.force = (200, b"not json")
        with self.assertRaisesRegex(RuntimeError, "invalid JSON"):
            db_connector.get_sensor_data(DEVEUI, "BatteryVoltage", DAY, DAY)

    def test_connection_refused(self):
        """an unreachable API raises RuntimeError (transport error)"""
        self.set_settings(backend_api_base_url=f"http://127.0.0.1:{closed_port()}/mdb/api/v1")
        with self.assertRaisesRegex(RuntimeError, "transport error"):
            db_connector.get_sensor_data(DEVEUI, "BatteryVoltage", DAY, DAY)

    def test_read_timeout(self):
        """an API that does not answer within the timeout raises RuntimeError (transport error), as now"""
        # Current behaviour, pinned so a change is noticed: the session's urllib3 Retry turns
        # the read timeout into MaxRetryError, which requests raises as ConnectionError, not
        # Timeout, so _api_get's QueryTimeout branch is not reached (the app then shows "An
        # unknown error occurred" instead of the timeout message).
        sock, port = silent_port()
        self.addCleanup(sock.close)
        self.set_settings(backend_api_base_url=f"http://127.0.0.1:{port}/mdb/api/v1",
                          backend_api_timeout_seconds=0.5)
        with self.assertRaisesRegex(RuntimeError, "transport error.*Read timed out"):
            db_connector.get_sensor_data(DEVEUI, "BatteryVoltage", DAY, DAY)

    def test_empty(self):
        """no rows in the range raises EmptyQuerySet"""
        with self.assertRaises(db_connector.EmptyQuerySet):
            db_connector.get_sensor_data(DEVEUI, "BatteryVoltage", "2025-01-01", "2025-01-02")

    def test_no_ground_sensors(self):
        """a device without groundtemp sensors in the metadata raises EmptyQuerySet, no measurements request"""
        self.server.fixtures["metadata"] = {"sensors": []}
        with self.assertRaises(db_connector.EmptyQuerySet):
            db_connector.get_ground_temp(DEVEUI, DAY, DAY)
        self.assertEqual(self.api_requests("measurements"), [])


class FrameTest(ApiTestCase):
    def test_sensor_frame(self):
        """measurements JSON becomes a UTC-indexed, sorted DataFrame with timeseries names as columns"""
        frame = db_connector.get_sensor_data(DEVEUI, "Weather Sensor", DAY, DAY)
        self.assertEqual(list(frame.columns), ["AirTemp", "RelHum", "BarometricPressure"])
        self.assertIsInstance(frame.index, pd.DatetimeIndex)
        self.assertEqual(str(frame.index.tz), "UTC")
        self.assertTrue(frame.index.is_monotonic_increasing)
        self.assertEqual(list(frame.index.strftime("%H:%M")), ["00:00", "01:00", "02:00"])
        self.assertEqual(list(frame["AirTemp"]), [-8.4, -8.9, -9.2])

    def test_ground_temp_frame(self):
        """get_ground_temp returns sensor names as columns and datatype None"""
        frame, datatype = db_connector.get_ground_temp(DEVEUI, DAY, DAY)
        self.assertIsNone(datatype)
        self.assertEqual(list(frame.columns), ["GroundTemp01", "GroundTemp02", "GroundTemp03"])
        self.assertEqual(list(frame["GroundTemp02"]), [-2.25, -2.3, -2.35])

    def test_depths(self):
        """get_ground_temp_sensor_depths maps groundtemp sensors to relative_z, shallow to deep"""
        self.assertEqual(
            list(db_connector.get_ground_temp_sensor_depths(DEVEUI).items()),
            [("GroundTemp01", -0.1), ("GroundTemp02", -0.25), ("GroundTemp03", -0.5)],
        )

    def test_async_wrapper(self):
        """the async wrapper used by the Dash callback returns the same DataFrame"""
        frame = asyncio.run(db_connector.get_sensor_data_async(DEVEUI, "BatteryVoltage", DAY, DAY))
        self.assertEqual(list(frame.columns), ["BatteryVoltage"])
        self.assertEqual(list(frame["BatteryVoltage"]), [3.71, 3.70, 3.69])


if __name__ == "__main__":
    unittest.main()
