"""Unit tests of "All loggers" on the phase 2 page (app/v2/callbacks.py, docs/plan/PLAN_api-v2.md,
T11): the dropdown option (only for a mast with 2 or more loggers), the date range over the
union of the periods, and the requests per action against the stub monitoring_db API: Daily
one /daily request set per logger overlapping the range, Full one /raw per logger with data in
the window plus one /daily per such logger for the profile, a zoom one /raw per logger with
data in the zoom window; always one logger after the other (the stub records the most
requests in flight), and an error stops the loggers after it. Callbacks are driven over HTTP
as in test_v2_page.py (fixtures: tests/fixtures/make_fixtures.py). Each test's docstring is
its name in the test output ("unit: v2_all_loggers: <docstring>")."""
import unittest

from support import fake_api
from test_v2_page import (CAMPBELL_M6, CAMPBELL_M13, TEKBOX_M6, PageTestCase, row_titles,
                          text_of)

from v2 import profile, texts  # noqa: E402

TEKBOX_M9 = "deveui:feedbeefcafe0001"          # device 20, SIS2024-03, 2024-10-14..18
CAMPBELL_M9 = "serial:NUKI2026-M9:12215"       # device 30, NUKI2026-M9, 2026-10-01..05
ALL_M6, ALL_M9 = "all:Mast 6", "all:Mast 9"
DELAY = 0.05        # seconds per stub request, so that overlapping requests would show

ALL_M6_ROWS = [texts.ROW_GROUND, texts.ROW_AIR, texts.ROW_HUMIDITY, texts.ROW_PRESSURE,
               "Inclination PitchAngle", "Inclination RollAngle", "Inclination XAngle",
               "Inclination YAngle", texts.ROW_HEADING, texts.ROW_SENSOR_TEMPERATURE, texts.ROW_BATTERY]


def selector(params):
    if params.get("deveui"):
        return "deveui:" + params["deveui"]
    return f"serial:{params['station']}:{params['serial']}"


class AllLoggersCase(PageTestCase):
    def setUp(self):
        super().setUp()
        self.catalogue = self.masts()["masts"]["data"]
        self.server.requests.clear()
        self.server.max_in_flight = 0

    def loggers(self, mast):
        r = self.call([("logger", "options"), ("logger", "value")], [("mast", "value", mast)],
                      state=[("masts", "data", self.catalogue)])
        return r["logger"]["options"], r["logger"]["value"]

    def load_mast(self, value, start, end, resolution="daily"):
        self.server.delay = DELAY
        try:
            return self.load_all(value, start, end, resolution, masts=self.catalogue)
        finally:
            self.server.delay = 0.0

    def measurements(self):
        """[(kind, logger value, start, end)] of the measurement requests, in order."""
        out = []
        for path, params in self.server.requests:
            if "/measurements/" in path:
                out.append((path.rsplit("/", 1)[-1], selector(params), params["start"], params["end"]))
        return out


class OptionTest(AllLoggersCase):
    def test_option_last(self):
        """the logger dropdown ends with "All loggers" for Mast 6 and Mast 9; the default stays the latest logger"""
        for mast, value, latest in (("Mast 6", ALL_M6, CAMPBELL_M6), ("Mast 9", ALL_M9, CAMPBELL_M9)):
            options, default = self.loggers(mast)
            self.assertEqual(options[-1], {"label": texts.LOGGER_ALL, "value": value})
            self.assertEqual(len(options), 3)
            self.assertEqual(default, latest)

    def test_no_option_single_logger(self):
        """Mast 13 (one logger with data): no "All loggers" option"""
        options, default = self.loggers("Mast 13")
        self.assertEqual([o["value"] for o in options], [CAMPBELL_M13])
        self.assertEqual(default, CAMPBELL_M13)

    def test_period_union(self):
        """All loggers: the date limits and default range cover the union of the loggers' periods"""
        props = ["min_date_allowed", "max_date_allowed", "start_date", "end_date", "initial_visible_month"]
        for value, period in ((ALL_M6, ["2026-09-01", "2026-10-05"]), (ALL_M9, ["2024-10-14", "2026-10-05"])):
            r = self.call([("period", p) for p in props], [("logger", "value", value)],
                          state=[("masts", "data", self.catalogue)])
            self.assertEqual([r["period"][p] for p in props], period * 2 + [period[1]])

    def test_all_without_catalogue(self):
        """All loggers without the catalogue (or of an unknown mast): a warning, no request"""
        fig, info, message, view, store = self.load_all(ALL_M6, "2026-09-01", "2026-10-05")
        self.assertIn(texts.CHOOSE_FIRST, text_of(message))
        self.assertIsNone(view)
        self.assertEqual(self.server.requests, [])


class DailyTest(AllLoggersCase):
    def test_daily_mast6(self):
        """Daily, Mast 6: one /daily per logger (its part of the range), one after the other; one trace per depth over both, a marker at the change"""
        fig, info, message, view, store = self.load_mast(ALL_M6, "2026-09-01", "2026-10-05")
        self.assertIsNone(message)
        self.assertEqual(self.measurements(), [
            ("daily", TEKBOX_M6, "2026-09-01T00:00:00Z", "2026-09-05T23:59:59Z"),
            ("daily", CAMPBELL_M6, "2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertEqual(self.server.max_in_flight, 1)
        self.assertEqual(row_titles(fig)[:len(ALL_M6_ROWS)], ALL_M6_ROWS)
        ground = [t for t in fig["data"] if t.get("yaxis") in (None, "y") and "customdata" in t]
        self.assertEqual([t["name"] for t in ground], ["-0.10 m", "-0.25 m", "-0.50 m"])
        for t in ground:
            self.assertEqual((t["x"][0], t["x"][-1]), ("2026-09-01", "2026-10-05"))
            self.assertIn(None, t["y"])
        shapes = fig["layout"]["shapes"]
        self.assertEqual([(s["x0"], s["line"]["dash"]) for s in shapes], [("2026-10-01 00:00:00", "dash")])
        self.assertIn("CR300_M6 (12096)", [a["text"] for a in fig["layout"]["annotations"]])
        self.assertIn("2 request(s)", text_of(info))
        self.assertIn(texts.INFO_LOGGERS.format(loggers="Tekbox_TBSL1_3 (feedbeefcafe0002); CR300_M6 (12096)"),
                      text_of(info))
        self.assertEqual(view["mode"], "daily")
        self.assertEqual([lg["value"] for lg in view["loggers"]], [TEKBOX_M6, CAMPBELL_M6])
        self.assertEqual((store["n"], store["depths"]), (35, [0.1, 0.25, 0.5]))
        self.assertEqual([c["logger"] for c in store["changes"]], [CAMPBELL_M6])

    def test_daily_one_logger_in_range(self):
        """Daily with a range of one logger only: one /daily request (the other logger is not asked), no marker"""
        fig, info, message, view, _ = self.load_mast(ALL_M6, "2026-10-01", "2026-10-05")
        self.assertEqual(self.measurements(),
                         [("daily", CAMPBELL_M6, "2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertFalse(fig["layout"].get("shapes"))
        self.assertIn("1 request(s)", text_of(info))

    def test_daily_no_logger(self):
        """a range without any logger: no request, a no-data message"""
        fig, info, message, view, _ = self.load_mast(ALL_M6, "2026-09-10", "2026-09-20")
        self.assertEqual(self.measurements(), [])
        self.assertIn(texts.NO_DATA_ALL.format(start="2026-09-10", end="2026-09-20"), text_of(message))
        self.assertIsNone(view)

    def test_daily_mast9(self):
        """Daily, Mast 9: one /daily per logger in turn; the two stations' series stay apart in legend groups named by station, in the same rows"""
        fig, info, message, view, store = self.load_mast(ALL_M9, "2024-10-14", "2026-10-05")
        self.assertEqual(self.measurements(), [
            ("daily", TEKBOX_M9, "2024-10-14T00:00:00Z", "2024-10-18T23:59:59Z"),
            ("daily", CAMPBELL_M9, "2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertEqual(self.server.max_in_flight, 1)
        titles = row_titles(fig)
        self.assertEqual(titles.count(texts.ROW_GROUND), 1)
        self.assertEqual(titles.count(texts.ROW_BATTERY), 1)
        ground = [t for t in fig["data"] if t.get("yaxis") in (None, "y") and "customdata" in t]
        self.assertEqual([t["legendgroup"] for t in ground], ["ground|SIS2024-03"] * 3 + ["ground|NUKI2026-M9"] * 3)
        self.assertEqual({t["legendgrouptitle"]["text"] for t in fig["data"] if t.get("legendgrouptitle")},
                         {"SIS2024-03", "NUKI2026-M9"})
        self.assertEqual(sorted(set(store["stations"])), ["NUKI2026-M9", "SIS2024-03"])
        self.assertEqual([s["x0"] for s in fig["layout"]["shapes"]], ["2026-10-01 00:00:00"])

    def test_error_stops(self):
        """an error on the first logger: its ApiError text, nothing shown, the second logger not fetched; on the second: nothing half-joined"""
        self.server.force_if = ({"deveui": "feedbeefcafe0002"}, fake_api.QUERY_TIMEOUT)
        fig, info, message, view, store = self.load_mast(ALL_M6, "2026-09-01", "2026-10-05")
        self.assertIn(texts.API_TIMEOUT, text_of(message))
        self.assertEqual([m[1] for m in self.measurements()], [TEKBOX_M6])
        self.assertIsNone(view)
        self.assertIsNone(store)
        self.assertEqual(fig["data"], [])
        self.server.requests.clear()
        self.server.force_if = ({"serial": "12096"}, (500, {"error": {"code": "INTERNAL_ERROR", "message": "boom",
                                                                      "details": {}}}))
        fig, info, message, view, store = self.load_mast(ALL_M6, "2026-09-01", "2026-10-05")
        self.assertIn("boom", text_of(message))
        self.assertEqual([m[1] for m in self.measurements()], [TEKBOX_M6, CAMPBELL_M6])
        self.assertIsNone(view)
        self.assertEqual(fig["data"], [])


class FullTest(AllLoggersCase):
    def test_full_mast6(self):
        """Full, Mast 6: one /raw per logger with data in the window, then one /daily per such logger for the profile, one at a time"""
        fig, info, message, view, store = self.load_mast(ALL_M6, "2026-09-01", "2026-10-05", "full")
        self.assertIsNone(message)
        self.assertEqual(self.measurements(), [
            ("raw", TEKBOX_M6, "2026-09-01T00:00:00Z", "2026-09-05T23:59:59Z"),
            ("raw", CAMPBELL_M6, "2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z"),
            ("daily", TEKBOX_M6, "2026-09-01T00:00:00Z", "2026-09-05T23:59:59Z"),
            ("daily", CAMPBELL_M6, "2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertEqual(self.server.max_in_flight, 1)
        self.assertIn("4 request(s)", text_of(info))
        self.assertIn("CR300_M6 (12096), " + texts.INFO_KIND_PROFILE, text_of(info))
        self.assertEqual(view["mode"], "full")
        self.assertEqual([t.get("meta") for t in fig["data"]], view["keys"])
        ground = next(t for t in fig["data"] if t["name"] == "-0.10 m")
        self.assertEqual((ground["x"][0], ground["x"][-1]), ("2026-09-01T00:00:00", "2026-10-05T00:00:00"))
        self.assertIn(None, ground["y"])
        self.assertEqual([s["x0"] for s in fig["layout"]["shapes"]], ["2026-10-01 00:00:00"])
        self.assertEqual(store["n"], 35)

    def test_full_mast9_window(self):
        """Full, Mast 9 over both periods: the one-year window holds only the Campbell: 1 /raw and 1 /daily"""
        fig, info, message, view, store = self.load_mast(ALL_M9, "2024-10-14", "2026-10-05", "full")
        self.assertEqual(self.measurements(), [
            ("raw", CAMPBELL_M9, "2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z"),
            ("daily", CAMPBELL_M9, "2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertIn(texts.INFO_FULL_LIMIT, text_of(info))
        self.assertEqual(view["range"], ["2025-10-06T00:00:00+00:00", "2026-10-05T23:59:59+00:00"])
        self.assertFalse(fig["layout"].get("shapes"))

    def test_full_profile_error(self):
        """Full: a failed profile request keeps the joined figure; the profile store has the error, the next logger's profile request is not made"""
        self.server.force_if = ({"path": "/measurements/daily", "deveui": "feedbeefcafe0002"}, fake_api.QUERY_TIMEOUT)
        fig, info, message, view, store = self.load_mast(ALL_M6, "2026-09-01", "2026-10-05", "full")
        self.assertIsNone(message)
        self.assertEqual(view["mode"], "full")
        self.assertEqual(store["error"], texts.API_TIMEOUT)
        self.assertEqual([m[:2] for m in self.measurements()],
                         [("raw", TEKBOX_M6), ("raw", CAMPBELL_M6), ("daily", TEKBOX_M6)])


class ZoomTest(AllLoggersCase):
    def full_view(self):
        fig, info, message, view, store = self.load_mast(ALL_M6, "2026-09-01", "2026-10-05", "full")
        self.server.requests.clear()
        self.server.max_in_flight = 0
        return view

    def zoom_mast(self, relayout, view):
        self.server.delay = DELAY
        try:
            return self.zoom(relayout, view)
        finally:
            self.server.delay = 0.0

    def test_zoom_both(self):
        """zoom over the change: one /raw per logger for its part of the window, one after the other, patched by joined key"""
        view = self.full_view()
        r = self.zoom_mast({"xaxis.range[0]": "2026-09-03 00:00:00", "xaxis.range[1]": "2026-10-02 06:00:00"}, view)
        self.assertEqual(self.measurements(), [
            ("raw", TEKBOX_M6, "2026-09-03T00:00:00Z", "2026-09-05T23:59:59Z"),
            ("raw", CAMPBELL_M6, "2026-10-01T00:00:00Z", "2026-10-02T06:00:00Z")])
        self.assertEqual(self.server.max_in_flight, 1)
        ops = r["data-graph"]["figure"]["operations"]
        self.assertEqual({op["location"][1] for op in ops if op["location"][-1] == "x"}, set(range(len(view["keys"]))))
        ground_x = next(op["params"]["value"] for op in ops if op["location"] == ["data", 0, "x"])
        self.assertEqual((ground_x[0], ground_x[-1]), ("2026-09-03T00:00:00", "2026-10-02T06:00:00"))
        self.assertIn("2 request(s)", text_of(r["info"]["children"]))

    def test_zoom_one(self):
        """zoom into one logger's period: one /raw request, for that logger only"""
        view = self.full_view()
        self.zoom_mast({"xaxis.range[0]": "2026-10-02 00:00:00", "xaxis.range[1]": "2026-10-03 00:00:00"}, view)
        self.assertEqual(self.measurements(),
                         [("raw", CAMPBELL_M6, "2026-10-02T00:00:00Z", "2026-10-03T00:00:00Z")])

    def test_zoom_gap(self):
        """zoom into the gap between the loggers: no request, the traces emptied"""
        view = self.full_view()
        r = self.zoom_mast({"xaxis.range[0]": "2026-09-10 00:00:00", "xaxis.range[1]": "2026-09-20 00:00:00"}, view)
        self.assertEqual(self.measurements(), [])
        ops = r["data-graph"]["figure"]["operations"]
        self.assertEqual({len(op["params"]["value"]) for op in ops if op["location"][-1] == "x"}, {0})
        self.assertIn("0 request(s)", text_of(r["info"]["children"]))

    def test_zoom_error(self):
        """a failed zoom request: the error text, the figure kept, the next logger not fetched"""
        view = self.full_view()
        self.server.force_if = ({"deveui": "feedbeefcafe0002"}, fake_api.QUERY_TIMEOUT)
        r = self.zoom_mast({"xaxis.range[0]": "2026-09-03 00:00:00", "xaxis.range[1]": "2026-10-02 06:00:00"}, view)
        self.assertNotIn("data-graph", r)
        self.assertIn(texts.API_TIMEOUT, text_of(r["message"]["children"]))
        self.assertEqual([m[1] for m in self.measurements()], [TEKBOX_M6])


class ProfileTest(AllLoggersCase):
    def draw(self, day_value, store):
        r = self.call([("profile-graph", "figure"), ("profile-label", "children"), ("data-graph", "figure")],
                      [("profile-day", "value", day_value), ("profile-envelope", "value", "both"),
                       ("profile", "data", store)])
        return r["profile-graph"]["figure"], r["profile-label"]["children"], r["data-graph"]["figure"]

    def test_profile_mast6(self):
        """profile from the joined ground temperatures (Mast 6): the Campbell on its days, the Tekbox on its days; the day marker keeps the change line; no request"""
        *_, store = self.load_mast(ALL_M6, "2026-09-01", "2026-10-05")
        self.server.requests.clear()
        fig, label, patch = self.draw(profile.day_number("2026-10-03"), store)
        mean = next(t for t in fig["data"] if t["meta"] == "mean")
        self.assertLess(mean["x"][0], -1.0)                     # Campbell node 1: -1.1 ...
        fig, label, _ = self.draw(profile.day_number("2026-09-03"), store)
        mean = next(t for t in fig["data"] if t["meta"] == "mean")
        self.assertGreater(mean["x"][0], -0.5)                  # Tekbox node 1: -0.1 ...
        shapes = patch["operations"][0]["params"]["value"]
        self.assertEqual(len(shapes), 2)
        self.assertEqual(shapes[1]["x0"], "2026-10-01 00:00:00")
        self.assertNotIn("station", label)
        self.assertEqual(self.server.requests, [])

    def test_profile_mast9(self):
        """profile of Mast 9 (two stations): the station whose logger has data on the chosen day, named in the label"""
        *_, store = self.load_mast(ALL_M9, "2024-10-14", "2026-10-05")
        fig, label, _ = self.draw(profile.day_number("2024-10-16"), store)
        self.assertIn(texts.PROFILE_LABEL_STATION.format(station="SIS2024-03"), label)
        mean = next(t for t in fig["data"] if t["meta"] == "mean")
        self.assertEqual(len(mean["y"]), 3)
        fig, label, _ = self.draw(profile.day_number("2026-10-03"), store)
        self.assertIn(texts.PROFILE_LABEL_STATION.format(station="NUKI2026-M9"), label)
        mean = next(t for t in fig["data"] if t["meta"] == "mean")
        self.assertEqual(len(mean["y"]), 3)
        self.assertLess(mean["x"][0], -3.0)


if __name__ == "__main__":
    unittest.main()
