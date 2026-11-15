"""Unit tests of the stub monitoring_db API itself (tests/fake_api.py) and its fixtures
(tests/fixtures/make_fixtures.py): v1/v2 metadata, v2 raw measurements, errors, gzip. Each
test's docstring is its name in the test output ("unit: fake_api: <docstring>")."""
import gzip
import importlib.util
import json
import threading
import unittest
import urllib.error
import urllib.request
from urllib.parse import urlencode

from support import FIXTURES, fake_api

RAW = "/mdb/api/v2/measurements/raw"
TB6 = "feedbeefcafe0002"          # Mast 6 Tekbox (device 10), data 2026-09-01..05
TB13 = "feedbeefcafe0008"         # Mast 13 Tekbox (device 40), no measurements


class StubTestCase(unittest.TestCase):
    def setUp(self):
        self.server = fake_api.make_server(FIXTURES, 0, "127.0.0.1")
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.root = f"http://127.0.0.1:{self.server.server_port}"

    def call(self, path, headers=None, **params):
        """(status, response headers, body as JSON); a gzip body is decompressed."""
        url = self.root + path + ("?" + urlencode(params) if params else "")
        req = urllib.request.Request(url, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                status, hdrs, raw = r.status, r.headers, r.read()
        except urllib.error.HTTPError as e:
            status, hdrs, raw = e.code, e.headers, e.read()
        if hdrs.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        return status, hdrs, json.loads(raw)

    def ok(self, path, **params):
        status, _h, body = self.call(path, **params)
        self.assertEqual(status, 200, body)
        return body

    def error(self, path, status, code, **params):
        got, _h, body = self.call(path, **params)
        self.assertEqual((got, body["error"]["code"]), (status, code), body)
        self.assertEqual(set(body["error"]), {"code", "message", "details"})
        return body["error"]

    def raw(self, **params):
        return self.ok(RAW, **params)

    @staticmethod
    def column(body, key):
        keys = [c["key"] for c in body["columns"]]
        return body["values"][keys.index(key)]


class MetadataTest(StubTestCase):
    def test_v1_fields(self):
        """v1 metadata has the real API's lists and fields, no time ranges and no fixture-only fields"""
        body = self.ok("/mdb/api/v1/metadata")
        self.assertEqual(set(body), {"stations", "recording_devices", "sensors", "timeseries", "sensor_types"})
        self.assertEqual(set(body["stations"][0]), {"id", "identifier", "name"})
        self.assertEqual(set(body["recording_devices"][0]),
                         {"id", "deveui", "display_name", "serial", "station_id", "station_identifier"})
        self.assertEqual(set(body["sensors"][0]),
                         {"id", "recording_device_deveui", "name", "type", "relative_z", "recording_device_id",
                          "recording_device_serial", "station_identifier"})
        self.assertEqual(set(body["timeseries"][0]), {"id", "sensor_id", "name"})
        self.assertIn("TBSCS1", body["sensor_types"])

    def test_v2_times(self):
        """v2 metadata adds first/last_measurement_time to devices and timeseries, null without data"""
        body = self.ok("/mdb/api/v2/metadata")
        devices = {d["id"]: d for d in body["recording_devices"]}
        self.assertEqual((devices[10]["first_measurement_time"], devices[10]["last_measurement_time"]),
                         ("2026-09-01T00:00:00Z", "2026-09-05T00:00:00Z"))
        self.assertEqual((devices[11]["first_measurement_time"], devices[11]["last_measurement_time"]),
                         ("2026-10-01T00:00:00Z", "2026-10-05T00:00:00Z"))
        self.assertEqual((devices[40]["first_measurement_time"], devices[40]["last_measurement_time"]), (None, None))
        series = {t["id"]: t for t in body["timeseries"]}
        self.assertEqual(series[11032]["name"], "XAngle")
        self.assertEqual(series[11032]["last_measurement_time"], "2026-10-05T00:00:00Z")
        self.assertIsNone(series[40011]["first_measurement_time"])
        self.assertNotIn("_datatype", series[11032])

    def test_v2_narrowing(self):
        """v2 metadata station/serial narrow the lists (station by identifier or by a unique name)"""
        body = self.ok("/mdb/api/v2/metadata", station="SIS2024-01")
        self.assertEqual([s["identifier"] for s in body["stations"]], ["SIS2024-01"])
        self.assertEqual(sorted(d["id"] for d in body["recording_devices"]), [10, 11])
        self.assertEqual({s["recording_device_id"] for s in body["sensors"]}, {10, 11})
        sensor_ids = {s["id"] for s in body["sensors"]}
        self.assertTrue(all(t["sensor_id"] in sensor_ids for t in body["timeseries"]))
        body = self.ok("/mdb/api/v2/metadata", station="SIS2024-07", serial="12096")
        self.assertEqual([d["id"] for d in body["recording_devices"]], [60])
        body = self.ok("/mdb/api/v1/metadata", station="Nukissiorfiit Mast 6")
        self.assertEqual(sorted(d["id"] for d in body["recording_devices"]), [10, 11])

    def test_metadata_errors(self):
        """metadata: unknown parameter 400, unknown station 404, shared serial or name 400 AMBIGUOUS_SELECTOR"""
        self.assertEqual(self.error("/mdb/api/v2/metadata", 400, "INVALID_QUERY", start="x")["details"],
                         {"params": ["start"]})
        self.error("/mdb/api/v2/metadata", 404, "STATION_NOT_FOUND", station="NOPE")
        self.error("/mdb/api/v2/metadata", 400, "AMBIGUOUS_SELECTOR", serial="12096")
        err = self.error("/mdb/api/v2/metadata", 400, "AMBIGUOUS_SELECTOR", station="Nukissiorfiit Mast 9")
        self.assertEqual(err["details"]["stations"], ["SIS2024-03", "NUKI2026-M9"])


class RawTest(StubTestCase):
    def test_shape(self):
        """v2 raw: columnar shape, values aligned with time, column details, meta"""
        body = self.raw(deveui=TB6, start="2026-09-01T00:00:00Z", end="2026-09-01T06:00:00Z")
        self.assertEqual(set(body), {"request", "time", "columns", "values", "meta"})
        self.assertEqual(body["time"], ["2026-09-01T00:00:00Z", "2026-09-01T03:00:00Z", "2026-09-01T06:00:00Z"])
        self.assertEqual(len(body["columns"]), 11)
        self.assertEqual(len(body["values"]), 11)
        self.assertTrue(all(len(v) == 3 for v in body["values"]))
        self.assertEqual(body["meta"], {
            "row_count": 3, "column_count": 11, "format": "columnar", "resolution": "raw",
            "sorted": "timestamp_asc", "timezone": "UTC", "column_key_source": "canonical_db_fields",
            "column_key_pattern": "<sensor.name>__<timeseries.name>", "calibrated": True})
        detail = next(c for c in body["columns"] if c["key"] == "Weather Sensor__BarometricPressure")
        self.assertEqual(detail, {
            "key": "Weather Sensor__BarometricPressure", "recording_device_id": 10, "recording_device_serial": "",
            "sensor_id": 1002, "sensor_name": "Weather Sensor", "sensor_type": "TBSHTP06", "relative_z": 2.0,
            "timeseries_id": 10022, "timeseries_name": "BarometricPressure", "datatype": "Pressure", "unit": "Pa",
            "calibration": None})
        self.assertEqual(self.column(body, "Weather Sensor__BarometricPressure"), [101300.0, 101310.0, 101320.0])
        self.assertEqual(self.column(body, "GroundTemp02__GroundTemperature"), [-0.2, -0.202, -0.204])
        self.assertEqual(body["request"]["start"], "2026-09-01T00:00:00Z")
        self.assertEqual(body["request"]["deveui"], TB6)

    def test_inclination_tekbox(self):
        """v2 raw: the Tekbox inclination sensor has Roll/Pitch (Angle), CompassHeading (Heading), SensorTemp"""
        body = self.raw(deveui=TB6, start="2026-09-01T00:00:00Z", end="2026-09-01T00:00:00Z",
                        sensor_name="Inclination Sensor")
        self.assertEqual({c["timeseries_name"]: c["datatype"] for c in body["columns"]},
                         {"CompassHeading": "Heading", "PitchAngle": "Angle", "RollAngle": "Angle",
                          "SensorTemp": "Temperature"})
        self.assertEqual({c["sensor_type"] for c in body["columns"]}, {"TBSCS1"})

    def test_inclusive(self):
        """v2 raw: start and end are inclusive; a range between two timestamps is empty"""
        body = self.raw(deveui=TB6, start="2026-09-01T03:00:00Z", end="2026-09-01T03:00:00Z")
        self.assertEqual(body["time"], ["2026-09-01T03:00:00Z"])
        body = self.raw(deveui=TB6, start="2026-09-01T03:00:01Z", end="2026-09-01T05:59:59+00:00")
        self.assertEqual((body["time"], body["columns"], body["values"], body["meta"]["row_count"]), ([], [], [], 0))

    def test_offset(self):
        """v2 raw: start/end with an offset are taken in UTC and echoed in UTC with Z"""
        body = self.raw(deveui=TB6, start="2026-09-01T05:00:00+02:00", end="2026-09-01T03:00:00-03:00")
        self.assertEqual(body["time"], ["2026-09-01T03:00:00Z", "2026-09-01T06:00:00Z"])
        self.assertEqual((body["request"]["start"], body["request"]["end"]),
                         ("2026-09-01T03:00:00Z", "2026-09-01T06:00:00Z"))

    def test_serial_station(self):
        """v2 raw: serial + station selects that station's device with a shared serial (keys get the serial prefix)"""
        body = self.raw(serial="12096", station="SIS2024-01", start="2026-10-01T00:00:00Z", end="2026-10-01T00:00:00Z")
        self.assertEqual({c["recording_device_id"] for c in body["columns"]}, {11})
        # As the real API: with station given (also together with serial) the keys are prefixed
        self.assertEqual(self.column(body, "12096__BatteryVoltage__BatteryVoltage"), [13.0])
        self.assertEqual(body["meta"]["column_key_pattern"], "<device>__<sensor.name>__<timeseries.name>")
        body = self.raw(serial="12096", station="SIS2024-07", start="2026-10-01T00:00:00Z", end="2026-10-01T00:00:00Z")
        self.assertEqual({c["recording_device_id"] for c in body["columns"]}, {60})
        body = self.raw(serial="12216", start="2026-10-01T00:00:00Z", end="2026-10-01T00:00:00Z")
        self.assertEqual({c["recording_device_id"] for c in body["columns"]}, {41})
        self.assertIn("BatteryVoltage__BatteryVoltage", [c["key"] for c in body["columns"]])

    def test_campbell_columns(self):
        """v2 raw: Campbell inclinometer X/YAngle every 6 h, null at the 3-hourly times of the other series"""
        body = self.raw(serial="12096", station="SIS2024-01", start="2026-10-01T00:00:00Z", end="2026-10-01T06:00:00Z")
        self.assertEqual(len(body["time"]), 3)
        self.assertEqual(self.column(body, "12096__Dual Axis Inclinometer__XAngle"), [0.45, None, 0.452])
        self.assertEqual(self.column(body, "12096__Dual Axis Inclinometer__YAngle"), [-0.3, None, -0.302])
        self.assertEqual(self.column(body, "12096__Weather Sensor__AirTemp"), [3.0, 2.95, 2.9])
        detail = next(c for c in body["columns"] if c["timeseries_name"] == "XAngle")
        self.assertEqual((detail["datatype"], detail["unit"], detail["sensor_type"]),
                         ("Angle", "deg", "Level Developments SOLAR-2-45-2-RS485M"))

    def test_no_data_columns(self):
        """v2 raw: a sensor without data in the range has no column; a device without data gives empty lists"""
        body = self.raw(serial="12096", station="SIS2024-01", start="2026-10-01T03:00:00Z", end="2026-10-01T03:00:00Z")
        self.assertEqual(len(body["time"]), 1)
        self.assertNotIn("Dual Axis Inclinometer", {c["sensor_name"] for c in body["columns"]})
        self.assertEqual(len(body["columns"]), 7)
        body = self.raw(deveui=TB13, start="2025-01-01T00:00:00Z", end="2026-11-01T00:00:00Z")
        self.assertEqual((body["time"], body["columns"], body["values"]), ([], [], []))

    def test_station_mode(self):
        """v2 raw: station alone gives every device, keys prefixed with the serial or device<id>"""
        body = self.raw(station="SIS2024-01", start="2026-09-05T00:00:00Z", end="2026-10-01T00:00:00Z",
                        sensor_name="BatteryVoltage")
        self.assertEqual(body["time"], ["2026-09-05T00:00:00Z", "2026-10-01T00:00:00Z"])
        self.assertEqual([c["key"] for c in body["columns"]],
                         ["device10__BatteryVoltage__BatteryVoltage", "12096__BatteryVoltage__BatteryVoltage"])
        self.assertEqual(body["values"], [[3.668, None], [None, 13.0]])
        self.assertEqual(body["meta"]["column_key_pattern"], "<device>__<sensor.name>__<timeseries.name>")
        self.assertEqual(body["request"]["station"], "SIS2024-01")

    def test_filters(self):
        """v2 raw: sensor_name__in and timeseries_name filter the columns"""
        body = self.raw(deveui=TB6, start="2026-09-01T00:00:00Z", end="2026-09-01T00:00:00Z",
                        sensor_name__in="GroundTemp01,GroundTemp03")
        self.assertEqual([c["sensor_name"] for c in body["columns"]], ["GroundTemp01", "GroundTemp03"])
        body = self.raw(deveui=TB6, start="2026-09-01T00:00:00Z", end="2026-09-01T00:00:00Z",
                        timeseries_name="GroundTemperature")
        self.assertEqual([c["relative_z"] for c in body["columns"]], [-0.1, -0.25, -0.5])
        body = self.raw(deveui=TB6, start="2026-09-01T00:00:00Z", end="2026-09-01T00:00:00Z", relative_z_max="0")
        self.assertEqual(len(body["columns"]), 3)

    def test_calibrated_masked(self):
        """v2 raw: calibrated=false is echoed in meta; include_masked=true adds all-false masked arrays"""
        body = self.raw(serial="12096", station="SIS2024-01", start="2026-10-01T00:00:00Z", end="2026-10-01T03:00:00Z",
                        calibrated="false", include_masked="true",
                        sensor_name__in="BatteryVoltage,Dual Axis Inclinometer")
        self.assertFalse(body["meta"]["calibrated"])
        self.assertEqual(body["masked"], [[False, False]] + [[False, None]] * 3)

    def test_errors(self):
        """v2 raw: 400 INVALID_QUERY / AMBIGUOUS_SELECTOR and 404 *_NOT_FOUND as the real API"""
        day = {"start": "2026-10-01T00:00:00Z", "end": "2026-10-01T00:00:00Z"}
        self.error(RAW, 400, "INVALID_QUERY", deveui=TB6, start="2026-10-02T00:00:00Z", end="2026-10-01T00:00:00Z")
        self.error(RAW, 400, "INVALID_QUERY", deveui=TB6, start="2026-10-01T00:00:00", end="2026-10-01T00:00:00Z")
        self.error(RAW, 400, "INVALID_QUERY", deveui=TB6, start="2026-10-01", end="2026-10-01T00:00:00Z")
        self.error(RAW, 400, "INVALID_QUERY", deveui=TB6, start="2026-10-01T00:00:00Z")
        self.error(RAW, 400, "INVALID_QUERY", **day)
        self.error(RAW, 400, "INVALID_QUERY", deveui=TB6, station="SIS2024-01", **day)
        self.error(RAW, 400, "INVALID_QUERY", deveui=TB6, calibrated="yes", **day)
        self.error(RAW, 400, "INVALID_QUERY", deveui=TB6, resolution="daily", **day)
        err = self.error(RAW, 400, "AMBIGUOUS_SELECTOR", serial="12096", **day)
        self.assertEqual(err["details"], {"serial": "12096", "stations": ["SIS2024-01", "SIS2024-07"]})
        self.error(RAW, 404, "DEVEUI_NOT_FOUND", deveui="0000000000000000", **day)
        self.error(RAW, 404, "SERIAL_NOT_FOUND", serial="99999", **day)
        self.error(RAW, 404, "SERIAL_NOT_FOUND", serial="12216", station="SIS2024-01", **day)
        self.error(RAW, 404, "STATION_NOT_FOUND", station="NOPE", **day)


class ServerTest(StubTestCase):
    def test_gzip(self):
        """v2 raw is gzip-compressed only when the request sends Accept-Encoding: gzip"""
        params = {"deveui": TB6, "start": "2026-09-01T00:00:00Z", "end": "2026-09-02T00:00:00Z"}
        status, hdrs, body = self.call(RAW, headers={"Accept-Encoding": "gzip, deflate"}, **params)
        self.assertEqual((status, hdrs.get("Content-Encoding")), (200, "gzip"))
        self.assertEqual(body["meta"]["row_count"], 9)
        status, hdrs, _body = self.call(RAW, **params)
        self.assertIsNone(hdrs.get("Content-Encoding"))
        _s, hdrs, _b = self.call("/mdb/api/v2/metadata", headers={"Accept-Encoding": "gzip"})
        self.assertIsNone(hdrs.get("Content-Encoding"))

    def test_force_and_record(self):
        """server.force answers every API path; server.requests and server.headers record each request"""
        self.server.force = fake_api.QUERY_TIMEOUT
        self.error(RAW, 504, "QUERY_TIMEOUT", deveui=TB6, start="x", end="y")
        self.error("/api/v2/metadata", 504, "QUERY_TIMEOUT")
        self.assertEqual(self.server.requests, [(RAW, {"deveui": TB6, "start": "x", "end": "y"}),
                                                ("/api/v2/metadata", {})])
        self.assertEqual(len(self.server.headers), 2)
        self.assertIn("host", self.server.headers[0])


class FixtureTest(unittest.TestCase):
    def test_up_to_date(self):
        """the fixture files equal the output of tests/fixtures/make_fixtures.py"""
        spec = importlib.util.spec_from_file_location("make_fixtures", FIXTURES / "make_fixtures.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for name, text in module.render().items():
            on_disk = (FIXTURES / name).read_text(encoding="utf-8").replace("\r\n", "\n")
            self.assertEqual(on_disk, text, f"{name} is stale: run python tests/fixtures/make_fixtures.py")


if __name__ == "__main__":
    unittest.main()
