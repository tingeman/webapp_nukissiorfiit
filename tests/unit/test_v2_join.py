"""Unit tests of the join of several loggers' series ("All loggers", app/v2/join.py and its
use in figures.py and profile.py; docs/plan/PLAN_api-v2.md, T11): join by depth and series
name within one station (Tekbox -> Campbell on Mast 6), stations kept apart (Mast 9), the
line breaks at a logger change, the change markers, and the profile's station per day. The
series come from the stub monitoring_db API (fixtures: tests/fixtures/make_fixtures.py) or
are built here. Each test's docstring is its name in the test output ("unit: v2_join:
<docstring>")."""
import unittest

import numpy as np

from support import ApiTestCase, ManualThreads, config

from test_v2_profile import assert_envelope_as_trumpet  # noqa: E402
from v2 import api_client, figures, join, profile, texts  # noqa: E402
from v2.api_client import Client, DailySeries, RawSeries  # noqa: E402

TEKBOX_M6 = "deveui:feedbeefcafe0002"          # device 10, SIS2024-01, 2026-09-01..05
CAMPBELL_M6 = "serial:SIS2024-01:12096"        # device 11, SIS2024-01, 2026-10-01..05
TEKBOX_M9 = "deveui:feedbeefcafe0001"          # device 20, SIS2024-03, 2024-10-14..18
CAMPBELL_M9 = "serial:NUKI2026-M9:12215"       # device 30, NUKI2026-M9, 2026-10-01..05


def logger(value, station, label=None):
    return {"value": value, "station": station, "label": label or f"{value} (x), 2026-01-01 to 2026-01-02"}


def daily(days, mean, name="GroundTemperature", z=-0.1, unit="C", datatype="Temperature", sensor="S1"):
    t = np.array(days, dtype="datetime64[D]")
    m = np.array(mean, dtype=float)
    details = {"key": f"{sensor}__{name}", "timeseries_name": name, "relative_z": z, "unit": unit,
               "datatype": datatype, "sensor_name": sensor}
    return DailySeries(details=details, time=t, mean=m, min=m - 0.5, max=m + 0.5,
                       count=np.full(len(t), 8, dtype=np.int64))


def raw(times, values, name="BatteryVoltage", unit="V"):
    details = {"key": f"Battery__{name}", "timeseries_name": name, "relative_z": 4.0, "unit": unit,
               "datatype": "Voltage", "sensor_name": "BatteryVoltage"}
    return RawSeries(details=details, time=np.array(times, dtype="datetime64[ms]"),
                     value=np.array(values, dtype=float))


class StubJoinCase(ApiTestCase):
    def setUp(self):
        super().setUp()
        self.set_settings(backend_api_daily_chunk_years=3)
        self.client = Client.from_settings(config.settings, start_thread=ManualThreads())
        self.masts = {m["name"]: self.client.mast(m["name"]) for m in self.client.masts()}

    def parts(self, mast, kind="daily", start="2024-01-01", end="2026-10-05"):
        fetch = getattr(self.client, kind)
        return [(lg, fetch(lg["value"], start, end).series) for lg in self.masts[mast]["loggers"]]


class JoinStubTest(StubJoinCase):
    def test_join_by_depth_one_station(self):
        """Mast 6 daily: Tekbox and Campbell ground temperatures join by depth into 3 series of 10 days each, broken at the change"""
        joined = join.join(self.parts("Mast 6"))
        ground = api_client.ground_temperature(joined.series)
        self.assertEqual([s.label for s in ground], ["-0.10 m", "-0.25 m", "-0.50 m"])
        for sel in ground:
            d = sel.series.details
            self.assertEqual(d["station"], "SIS2024-01")
            self.assertEqual(d["loggers"], [TEKBOX_M6, CAMPBELL_M6])
            self.assertEqual(d["breaks"], [5])
            self.assertEqual(len(sel.series.time), 10)
            self.assertEqual(str(sel.series.time[0]), "2026-09-01")
            self.assertEqual(str(sel.series.time[-1]), "2026-10-05")
        top = ground[0].series        # Tekbox node 1 starts at -0.1, the Campbell's at -1.1
        self.assertAlmostEqual(top.max[0], -0.1, places=4)
        self.assertAlmostEqual(top.max[5], -1.1, places=4)

    def test_join_by_series_name(self):
        """Mast 6: battery, weather, sensor temperature join by name; Roll/Pitch and X/Y stay separate (4 angle series); heading is the Tekbox's only"""
        joined = join.join(self.parts("Mast 6"))
        s = joined.series

        def loggers_of(items):
            return [sel.series.details["loggers"] for sel in items]
        both = [TEKBOX_M6, CAMPBELL_M6]
        self.assertEqual(loggers_of(api_client.battery(s)), [both])
        for name in ("AirTemp", "RelHum", "BarometricPressure"):
            self.assertEqual(loggers_of(api_client.weather(s, name)), [both], name)
        self.assertEqual(loggers_of(api_client.sensor_temperature(s)), [both])
        angles = api_client.inclination(s)
        self.assertEqual(sorted(a.label for a in angles), ["PitchAngle", "RollAngle", "XAngle", "YAngle"])
        self.assertEqual(sorted(len(a.series.details["loggers"]) for a in angles), [1, 1, 1, 1])
        self.assertEqual(loggers_of(api_client.heading(s)), [[TEKBOX_M6]])
        pressure = api_client.weather(s, "BarometricPressure")[0]
        self.assertEqual(pressure.unit, "kPa")
        self.assertAlmostEqual(float(pressure.series.mean[0]), 101.3, delta=0.1)

    def test_station_separation(self):
        """Mast 9: the Tekbox (SIS2024-03) and Campbell (NUKI2026-M9) series stay apart: 6 ground temperature series, 3 per station, no breaks"""
        joined = join.join(self.parts("Mast 9"))
        ground = api_client.ground_temperature(joined.series)
        self.assertEqual(len(ground), 6)
        by_station = {}
        for sel in ground:
            self.assertEqual(sel.series.details["breaks"], [])
            by_station.setdefault(join.station(sel), []).append(sel.label)
        self.assertEqual(by_station, {"SIS2024-03": ["-0.10 m", "-0.25 m", "-0.50 m"],
                                      "NUKI2026-M9": ["-0.10 m", "-0.25 m", "-0.50 m"]})
        self.assertEqual(len(api_client.battery(joined.series)), 2)
        self.assertEqual([c["station"] for c in joined.changes], ["NUKI2026-M9"])

    def test_changes(self):
        """the logger change is at the later logger's first measurement (daily: its first day; raw: its first time), labelled with its name"""
        for kind in ("daily", "raw"):
            joined = join.join(self.parts("Mast 6", kind, "2026-09-01", "2026-10-05"))
            self.assertEqual(joined.changes, [{"time": "2026-10-01T00:00:00", "logger": CAMPBELL_M6,
                                               "label": "CR300_M6 (12096)", "station": "SIS2024-01"}], kind)
        # only the Campbell has data in the range: no change
        joined = join.join(self.parts("Mast 6", "daily", "2026-10-01", "2026-10-05"))
        self.assertEqual(joined.changes, [])
        self.assertEqual([lg["value"] for lg in joined.loggers], [CAMPBELL_M6])

    def test_figure_markers(self):
        """data_figure of joined series: a dashed line over all rows and the logger name at each change"""
        joined = join.join(self.parts("Mast 6"))
        fig, keys, _ = figures.data_figure(figures.rows(joined.series), "daily", "r", changes=joined.changes)
        shapes = [s for s in fig.layout.shapes if s.name == "logger-change"]
        self.assertEqual(len(shapes), 1)
        self.assertEqual((shapes[0].x0, shapes[0].x1, shapes[0].yref, shapes[0].y0, shapes[0].y1),
                         ("2026-10-01 00:00:00", "2026-10-01 00:00:00", "paper", 0, 1))
        self.assertEqual(shapes[0].line.dash, "dash")
        labels = [a.text for a in fig.layout.annotations if a.name == "logger-change"]
        self.assertEqual(labels, [texts.CHANGE_LABEL.format(logger="CR300_M6 (12096)")])
        titles = [a.text.removesuffix(", daily mean") for a in fig.layout.annotations if a.name != "logger-change"]
        self.assertEqual(titles, [texts.ROW_GROUND, texts.ROW_AIR, texts.ROW_HUMIDITY, texts.ROW_PRESSURE,
                                  "Inclination PitchAngle", "Inclination RollAngle", "Inclination XAngle",
                                  "Inclination YAngle", texts.ROW_HEADING, texts.ROW_SENSOR_TEMPERATURE,
                                  texts.ROW_BATTERY])

    def test_figure_station_groups(self):
        """Mast 9 figure: in each row the traces are grouped by station (legend group titled with it), the ground colours per depth repeat per station"""
        joined = join.join(self.parts("Mast 9"))
        fig, keys, _ = figures.data_figure(figures.rows(joined.series), "daily", "r")
        means = [t for t in fig.data if t.customdata is not None]
        ground = [t for t in means if t.yaxis in (None, "y")]
        self.assertEqual([t.legendgroup for t in ground], ["ground|SIS2024-03"] * 3 + ["ground|NUKI2026-M9"] * 3)
        titles = [t.legendgrouptitle.text for t in fig.data if t.legendgrouptitle.text]
        self.assertIn("SIS2024-03", titles)
        self.assertIn("NUKI2026-M9", titles)
        self.assertEqual([t.line.color for t in ground[:3]], [t.line.color for t in ground[3:]])
        self.assertEqual({t.line.dash for t in ground[3:]}, {"dot"})
        battery = [t for t in means if "Battery" in (t.hovertemplate or "")]
        self.assertEqual(len({t.legendgroup for t in battery}), 2)
        self.assertTrue(all(t.showlegend for t in means))

    def test_profile_station_per_day(self):
        """Mast 9 profile store: a day of the Tekbox uses SIS2024-03, a day of the Campbell NUKI2026-M9, a day between them the latest station before it"""
        joined = join.join(self.parts("Mast 9"))
        store = profile.store_data(api_client.ground_temperature(joined.series), "all:Mast 9")
        self.assertEqual(len(store["keys"]), 6)
        self.assertEqual(sorted(store["stations"]), ["NUKI2026-M9"] * 3 + ["SIS2024-03"] * 3)
        day = profile.day_number

        def drawn(d):
            i = day(d) - store["first"]
            station = store["station_list"][store["day_station"][i]]
            rows = [r for r, s in enumerate(store["stations"]) if s == station]
            return station, [store["depths"][r] for r in rows if store["mean"][r][i] is not None], \
                [store["mean"][r][i] for r in rows if store["mean"][r][i] is not None]
        station, depths, mean = drawn("2024-10-16")
        self.assertEqual((station, depths), ("SIS2024-03", [0.1, 0.25, 0.5]))
        self.assertLess(mean[0], -2.0)             # Tekbox node 1: -0.1 - 2 (offset)
        self.assertGreater(mean[0], -2.5)
        station, depths, mean = drawn("2026-10-03")
        self.assertEqual((station, depths), ("NUKI2026-M9", [0.1, 0.25, 0.5]))
        self.assertLess(mean[0], -3.0)             # Campbell node 1: -0.1 - 3
        self.assertEqual(drawn("2025-06-01")[0], "SIS2024-03")
        self.assertEqual(drawn("2026-09-30")[0], "SIS2024-03")     # the day before the switch
        # one station: no per-day station
        single = profile.store_data(api_client.ground_temperature(join.join(self.parts("Mast 6")).series), "x")
        self.assertNotIn("day_station", single)

    def test_profile_envelope_as_trumpet(self):
        """Mast 9 profile store: the annual envelope of the day's station equals trumpet.trumpet's, at the record start, between the loggers and around the station switch"""
        joined = join.join(self.parts("Mast 9"))
        selected = api_client.ground_temperature(joined.series)
        store = profile.store_data(selected, "all:Mast 9")
        m = profile.matrices(selected)
        day = profile.day_number
        for d in ("2024-10-14", "2024-10-15", "2024-10-18", "2024-12-01", "2025-10-15", "2025-10-19",
                  "2026-09-30", "2026-10-01", "2026-10-03", "2026-10-05"):
            i = day(d) - store["first"]
            station = store["station_list"][store["day_station"][i]]
            rows = [r for r, s in enumerate(store["stations"]) if s == station]
            with self.subTest(day=d, station=station):
                assert_envelope_as_trumpet(self, store, m, i, rows)
        i = day("2025-10-19") - store["first"]          # 366 days after the Tekbox's last day
        self.assertEqual(store["valid"][i], 0)


class JoinSyntheticTest(unittest.TestCase):
    def test_daily_break_at_adjacent_change(self):
        """daily: two loggers on consecutive days still get a break (None) at the change; a single logger's line has none"""
        a = logger("a", "S")
        b = logger("b", "S")
        joined = join.join([(a, {"k": daily(["2026-01-01", "2026-01-02"], [1, 2])}),
                            (b, {"k2": daily(["2026-01-03", "2026-01-04"], [3, 4])})])
        (series,) = joined.series.values()
        d = figures.daily_trace_data(series)
        self.assertEqual(d["x"], ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-03", "2026-01-04"])
        self.assertEqual(d["mean"], [1.0, 2.0, None, 3.0, 4.0])
        self.assertEqual(d["min"][2], None)
        single = figures.daily_trace_data(daily(["2026-01-01", "2026-01-02", "2026-01-03"], [1, 2, 3]))
        self.assertEqual(single["mean"], [1.0, 2.0, 3.0])

    def test_raw_break_and_overlap(self):
        """raw: a break between the loggers' segments even when they overlap in time; each segment thinned over the window"""
        a = logger("a", "S")
        b = logger("b", "S")
        ta = ["2026-01-01T00:00", "2026-01-01T01:00", "2026-01-01T02:00"]
        tb = ["2026-01-01T01:30", "2026-01-01T03:00"]
        joined = join.join([(a, {"k": raw(ta, [1, 2, 3])}), (b, {"k": raw(tb, [4, 5])})])
        (series,) = joined.series.values()
        self.assertEqual(series.details["breaks"], [3])
        self.assertEqual(join.segments(series), [(0, 3), (3, 5)])
        sel = api_client.battery(joined.series)[0]
        lo, hi = np.datetime64("2026-01-01T00:00"), np.datetime64("2026-01-01T04:00")
        d = figures.raw_trace_data(sel, lo, hi)
        self.assertEqual(d["y"], [1.0, 2.0, 3.0, None, 4.0, 5.0])
        self.assertEqual(d["x"][3], "2026-01-01T01:30:00")
        self.assertEqual(joined.changes[0]["time"], "2026-01-01T01:30:00")

    def test_join_rule(self):
        """not joined: other stations, other units (Pa vs hPa), other depths; a key repeated in one logger gets the sensor name"""
        a, b, c = logger("a", "S1"), logger("b", "S1"), logger("c", "S2")
        joined = join.join([
            (a, {"p": daily(["2026-01-01"], [1013], name="BarometricPressure", unit="hPa", z=2.0),
                 "g1": daily(["2026-01-01"], [1], z=-0.1), "g2": daily(["2026-01-01"], [1], z=-0.2),
                 "s1": daily(["2026-01-01"], [1], name="SensorTemp", sensor="Inc1", z=0.5),
                 "s2": daily(["2026-01-01"], [1], name="SensorTemp", sensor="Inc2", z=0.5)}),
            (b, {"p": daily(["2026-01-02"], [101300], name="BarometricPressure", unit="Pa", z=2.0),
                 "g1": daily(["2026-01-02"], [1], z=-0.1), "g3": daily(["2026-01-02"], [1], z=-0.3),
                 "s1": daily(["2026-01-02"], [1], name="SensorTemp", sensor="Inc1", z=0.5)}),
            (c, {"g1": daily(["2026-01-03"], [1], z=-0.1)}),
        ])
        loggers = {k: s.details["loggers"] for k, s in joined.series.items()}
        self.assertEqual(loggers["S1|GroundTemperature|Temperature|C|-0.1"], ["a", "b"])
        self.assertEqual(loggers["S2|GroundTemperature|Temperature|C|-0.1"], ["c"])
        self.assertEqual(loggers["S1|GroundTemperature|Temperature|C|-0.2"], ["a"])
        self.assertEqual(loggers["S1|GroundTemperature|Temperature|C|-0.3"], ["b"])
        self.assertEqual(loggers["S1|BarometricPressure|Temperature|hPa|"], ["a"])
        self.assertEqual(loggers["S1|BarometricPressure|Temperature|Pa|"], ["b"])
        self.assertEqual(loggers["S1|SensorTemp|Temperature|C||Inc1"], ["a"])
        self.assertEqual(loggers["S1|SensorTemp|Temperature|C||Inc2"], ["a"])
        self.assertEqual(loggers["S1|SensorTemp|Temperature|C|"], ["b"])
        kpa = sorted(round(float(s.series.mean[0]), 6) for s in api_client.weather(joined.series, "BarometricPressure"))
        self.assertEqual(kpa, [101.3, 101.3])
        self.assertEqual([c_["logger"] for c_ in joined.changes], ["b", "c"])

    def test_short_label(self):
        """a logger's short label is its catalogue label without the period"""
        self.assertEqual(join.short_label({"label": "CR300_M6 (12096), 2026-10-01 to 2026-10-05"}),
                         "CR300_M6 (12096)")
        self.assertEqual(join.short_label({"label": "X"}), "X")
        self.assertEqual(join.short_label({"value": "deveui:1"}), "deveui:1")

    def test_empty(self):
        """loggers without data: no series and no change; a logger without data between two others is skipped"""
        a, b, c = logger("a", "S"), logger("b", "S"), logger("c", "S")
        empty = daily([], [])
        joined = join.join([(a, {"k": daily(["2026-01-01"], [1])}), (b, {"k": empty}), (c, {})])
        self.assertEqual([s.details["loggers"] for s in joined.series.values()], [["a"]])
        self.assertEqual(joined.changes, [])
        self.assertEqual(join.join([]).series, {})


if __name__ == "__main__":
    unittest.main()
