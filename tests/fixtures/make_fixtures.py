"""Writes the fixtures of the stub monitoring_db API (tests/fake_api.py):

    python tests/fixtures/make_fixtures.py          (from the repository root, any python 3.9+)

  metadata.json         stations, recording_devices, sensors, timeseries, sensor_types in the
                        shape of GET /api/v1/metadata. Timeseries also carry "_datatype" and
                        "_unit" (fixture-only fields, used for the v2 column details; the stub
                        does not serve them in the metadata, like the real API).
  measurements_v2.json  {"<timeseries id>": [["<time>Z", value], ...]}, ascending, for the v2
                        endpoints and the first/last times of /api/v2/metadata.

The committed files must equal this script's output (a unit test checks it). The data is
modelled on the dev station definitions of monitoring_db (00_assets/mdb_dev_ingestion_data/
dev-definitions/station_defs, SIS2024-01, -03, -05, NUKI2026-M9), shortened to a few days:

  station (id)        name                     device id: display name, deveui / serial, data
  SIS2024-01 (1)      Nukissiorfiit Mast 6     10: Tekbox feedbeefcafe0002 / "", 2026-09-01..05
                                               11: Campbell null / 12096, 2026-10-01..05
  SIS2024-03 (2)      Nukissiorfiit Mast 9     20: Tekbox feedbeefcafe0001 / null, 2024-10-14..18
  NUKI2026-M9 (3)     Nukissiorfiit Mast 9     30: Campbell "" / 12215, 2026-10-01..05
  SIS2024-05 (4)      Nukissiorfiit Mast 13    40: Tekbox feedbeefcafe0008 / "", no measurements
                                               41: Campbell null / 12216, 2026-10-01..05
  QAA2017-01 (5)      Qaanaaq borehole 1       50: Geoprecision null / A522C6, 2026-10-01..05
  SIS2024-07 (6)      Sisimiut test borehole   60: Campbell null / 12096 (same serial as 11),
                                                   2026-10-01..02

Every value is at a 3-hourly time (00:00, 03:00, ...), both ends of the period included, except
the Campbell inclinometer of device 11 ("Dual Axis Inclinometer"), which is 6-hourly: in a
response with the other series of that device its cells are null at 03:00, 09:00, ...

Ids: sensor id = device id * 100 + n, timeseries id = sensor id * 10 + m (n, m from 1), e.g.
timeseries 10031 is device 10, sensor 3 (Inclination Sensor), series 1 (CompassHeading).

Values: base + slope * k, rounded to 4 decimals, with k the number of 3-hour steps since the
start of the device's period (see the sensor groups). Ground temperature node i: -0.1 * i -
offset - 0.002 * k; AirTemp 2.0 + offset - 0.05 * k (offset per device: 0 for device 10,
1 for 11, 2 for 20, ..., 7 for 60). Battery: Tekbox 3.7 - 0.001 * k V, Campbell 13.0 -
0.002 * k V. Pressure in Pa: 101300 + 10 * k.
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUT = Path(__file__).resolve().parent
STEP = timedelta(hours=3)

STATIONS = [
    (1, "SIS2024-01", "Nukissiorfiit Mast 6"),
    (2, "SIS2024-03", "Nukissiorfiit Mast 9"),
    (3, "NUKI2026-M9", "Nukissiorfiit Mast 9"),
    (4, "SIS2024-05", "Nukissiorfiit Mast 13"),
    (5, "QAA2017-01", "Qaanaaq borehole 1"),
    (6, "SIS2024-07", "Sisimiut test borehole"),
]

# Sensor groups: (sensor name, type, relative_z, [(timeseries, datatype, unit, base, slope)])
BATTERY_TB = ("BatteryVoltage", "Internal", 4.0, [("BatteryVoltage", "Voltage", "V", 3.7, -0.001)])
BATTERY_CS = ("BatteryVoltage", "Internal", 4.0, [("BatteryVoltage", "Voltage", "V", 13.0, -0.002)])
WEATHER = ("Weather Sensor", "TBSHTP06", 2.0, [
    ("AirTemp", "Temperature", "C", 2.0, -0.05),
    ("BarometricPressure", "Pressure", "Pa", 101300.0, 10.0),
    ("RelHum", "Relative Humidity", "%", 80.0, 0.1),
])
INCLINATION_TB = ("Inclination Sensor", "TBSCS1", 0.5, [
    ("CompassHeading", "Heading", "deg", 182.0, 0.1),
    ("PitchAngle", "Angle", "deg", -0.8, -0.001),
    ("RollAngle", "Angle", "deg", 1.2, 0.001),
    ("SensorTemp", "Temperature", "C", 3.5, 0.01),
])
INCLINATION_CS = ("Dual Axis Inclinometer", "Level Developments SOLAR-2-45-2-RS485M", 0.5, [
    ("SensorTemp", "Temperature", "C", 3.0, 0.01),
    ("XAngle", "Angle", "deg", 0.45, 0.001),
    ("YAngle", "Angle", "deg", -0.3, -0.001),
])
GROUND_Z = [-0.10, -0.25, -0.50, -0.75, -1.00]


def ground(n, z=GROUND_Z, offset=0.0, sensor_type="Thermistor"):
    return [(f"GroundTemp{i:02d}", sensor_type, z[i - 1],
             [("GroundTemperature", "Temperature", "C", round(-0.1 * i - offset, 4), -0.002)])
            for i in range(1, n + 1)]


def t(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# (id, station id, display name, deveui, serial, sensors, (first, last) or None, offset,
#  {sensor name: step multiple})
DEVICES = [
    (10, 1, "Tekbox_TBSL1_3", "feedbeefcafe0002", "",
     [BATTERY_TB, WEATHER, INCLINATION_TB] + ground(3), ("2026-09-01T00:00:00Z", "2026-09-05T00:00:00Z"), 0.0, {}),
    (11, 1, "CR300_M6", None, "12096",
     [BATTERY_CS, WEATHER, INCLINATION_CS] + ground(3, offset=1.0), ("2026-10-01T00:00:00Z", "2026-10-05T00:00:00Z"), 1.0,
     {"Dual Axis Inclinometer": 2}),
    (20, 2, "Tekbox_TBSL1_2", "feedbeefcafe0001", None,
     [BATTERY_TB, WEATHER, INCLINATION_TB] + ground(3, offset=2.0), ("2024-10-14T00:00:00Z", "2024-10-18T00:00:00Z"), 2.0, {}),
    (30, 3, "CR300_M9", "", "12215",
     [BATTERY_CS, WEATHER, INCLINATION_CS] + ground(3, offset=3.0), ("2026-10-01T00:00:00Z", "2026-10-05T00:00:00Z"), 3.0, {}),
    (40, 4, "TekBox_Mast13", "feedbeefcafe0008", "",
     [BATTERY_TB, WEATHER, INCLINATION_TB] + ground(3, offset=4.0), None, 4.0, {}),
    (41, 4, "CR300_M13", None, "12216",
     [BATTERY_CS, WEATHER, INCLINATION_CS] + ground(4, offset=5.0), ("2026-10-01T00:00:00Z", "2026-10-05T00:00:00Z"), 5.0, {}),
    (50, 5, "QAA2017-01_A522C6", None, "A522C6",
     ground(3, z=[-1.0, -2.0, -3.0], offset=6.0), ("2026-10-01T00:00:00Z", "2026-10-05T00:00:00Z"), 6.0, {}),
    (60, 6, "CR300_SIS2024-07", None, "12096",
     [BATTERY_CS] + ground(3, offset=7.0), ("2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z"), 7.0, {}),
]


def build():
    station_ident = {sid: ident for sid, ident, _ in STATIONS}
    devices, sensors, timeseries, measurements = [], [], [], {}
    for dev_id, sid, display, deveui, serial, groups, period, offset, every in DEVICES:
        devices.append({"id": dev_id, "deveui": deveui, "display_name": display, "serial": serial,
                        "station_id": sid, "station_identifier": station_ident[sid]})
        for n, (sname, stype, z, series) in enumerate(groups, start=1):
            sensor_id = dev_id * 100 + n
            sensors.append({"id": sensor_id, "recording_device_deveui": deveui, "name": sname, "type": stype,
                            "relative_z": z, "recording_device_id": dev_id, "recording_device_serial": serial,
                            "station_identifier": station_ident[sid]})
            for m, (tsname, dtype, unit, base, slope) in enumerate(series, start=1):
                ts_id = sensor_id * 10 + m
                if tsname == "AirTemp":   # tells the loggers apart in the weather data too
                    base = base + offset
                timeseries.append({"id": ts_id, "sensor_id": sensor_id, "name": tsname,
                                   "_datatype": dtype, "_unit": unit})
                if period is None:
                    continue
                first, last = t(period[0]), t(period[1])
                points, k = [], 0
                while first + k * STEP <= last:
                    if k % every.get(sname, 1) == 0:
                        when = (first + k * STEP).strftime("%Y-%m-%dT%H:%M:%SZ")
                        points.append([when, round(base + slope * k, 4)])
                    k += 1
                measurements[str(ts_id)] = points
    # The real API's order: stations by identifier, devices by deveui (NULLs last), sensors by
    # device deveui and name, timeseries by sensor and name
    nulls_last = lambda v: (v is None, v or "")  # noqa: E731
    metadata = {
        "stations": [{"id": sid, "identifier": ident, "name": name}
                     for sid, ident, name in sorted(STATIONS, key=lambda s: (s[1], s[0]))],
        "recording_devices": sorted(devices, key=lambda d: (nulls_last(d["deveui"]), d["id"])),
        "sensors": sorted(sensors, key=lambda s: (nulls_last(s["recording_device_deveui"]), s["name"], s["id"])),
        "timeseries": sorted(timeseries, key=lambda x: (x["sensor_id"], x["name"], x["id"])),
        "sensor_types": sorted({s["type"] for s in sensors if s["type"]}),
    }
    return metadata, measurements


def render():
    """{file name: text} of the fixture files (one list item / timeseries per line)."""
    metadata, measurements = build()
    lines = ["{"]
    keys = list(metadata)
    for i, key in enumerate(keys):
        items = metadata[key]
        lines.append(f'  "{key}": [')
        lines.append(",\n".join("    " + json.dumps(item) for item in items))
        lines.append("  ]" + ("," if i < len(keys) - 1 else ""))
    lines.append("}")
    meta_text = "\n".join(lines) + "\n"
    meas_text = "{\n" + ",\n".join(f'  "{k}": {json.dumps(v)}' for k, v in measurements.items()) + "\n}\n"
    return {"metadata.json": meta_text, "measurements_v2.json": meas_text}


if __name__ == "__main__":
    for name, text in render().items():
        (OUT / name).write_text(text, encoding="utf-8", newline="\n")
        print(f"wrote {OUT / name} ({len(text)} bytes)")
