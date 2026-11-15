"""Unit tests of the ground temperature profile of the phase 2 page (app/v2/profile.py, the
profile part of layout.py, callbacks.py and figures.py; docs/plan/PLAN_api-v2.md, T10): the
profile store built by the load callback, the day slider, the three envelope choices, the
day marker on the data figure, and the API requests each action makes (slider and envelope:
none; Full: 1 raw + 1 daily; zoom: 1 raw). Callbacks are driven over HTTP as in
test_v2_page.py, against the stub monitoring_db API (fixtures: tests/fixtures/make_fixtures.py).
Each test's docstring is its name in the test output ("unit: v2_profile: <docstring>")."""
import datetime as dt
import json
import unittest
from unittest import mock

import numpy as np

from support import ApiTestCase, config  # noqa: F401  (support sets up sys.path)
from test_v2_page import CAMPBELL_M6, CAMPBELL_M13, PageTestCase, text_of

import app  # noqa: E402
from v2 import api_client, figures, profile, texts  # noqa: E402

M13_DEPTHS = [0.1, 0.25, 0.5, 0.75]


def day(s: str) -> int:
    return profile.day_number(s)


class ProfileCase(PageTestCase):
    def setup_profile(self, store):
        r = self.call([("profile-day", "min"), ("profile-day", "max"), ("profile-day", "marks"),
                       ("profile-day", "value"), ("profile-body", "style"), ("profile-note", "children")],
                      [("profile", "data", store)])
        return r["profile-day"], r["profile-body"]["style"], r["profile-note"]["children"]

    def draw(self, day_value, envelope, store):
        """draw_profile: (profile figure, label, data-graph patch or None)."""
        r = self.call([("profile-graph", "figure"), ("profile-label", "children"), ("data-graph", "figure")],
                      [("profile-day", "value", day_value), ("profile-envelope", "value", envelope),
                       ("profile", "data", store)])
        return (r["profile-graph"]["figure"], r["profile-label"]["children"],
                r.get("data-graph", {}).get("figure"))

    def expected_daily(self, logger, start, end):
        """The stub's /daily ground temperatures (via the client; clears the request log)."""
        result = self.client.daily(logger, start, end)
        self.server.requests.clear()
        return api_client.ground_temperature(result.series)


def meta(fig):
    return [t.get("meta") for t in fig["data"]]


def trace(fig, name):
    return next(t for t in fig["data"] if t.get("meta") == name)


# ---- layout -----------------------------------------------------------------------------------

class ProfileLayoutTest(unittest.TestCase):
    def setUp(self):
        self.by_id = {c.id: c for c in app.v2_app.layout._traverse() if getattr(c, "id", None)}

    def test_envelope_default(self):
        """the envelope selector offers Day min/max (default), Annual envelope and Both, persisted per session"""
        env = self.by_id["profile-envelope"]
        self.assertEqual(env.value, "day")
        self.assertEqual([o["value"] for o in env.options], ["day", "annual", "both"])
        self.assertEqual([o["label"] for o in env.options], [texts.ENV_DAY, texts.ENV_ANNUAL, texts.ENV_BOTH])
        self.assertTrue(env.persistence)
        self.assertEqual(env.persistence_type, "session")

    def test_slider_and_hidden_body(self):
        """the day slider shows its date in a tooltip (assets/profile.js) and the profile body starts hidden"""
        slider = self.by_id["profile-day"]
        self.assertEqual(slider.tooltip["transform"], "nukiDay")
        self.assertFalse(slider.allow_direct_input)
        self.assertEqual(self.by_id["profile-body"].style, {"display": "none"})
        r = app.server.test_client().get("/app/nuki/v2/assets/profile.js")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"dccFunctions.nukiDay", r.get_data())

    def test_profile_after_figure(self):
        """the profile card comes after the data figure"""
        ids = [getattr(c, "id", None) for c in app.v2_app.layout._traverse()]
        self.assertLess(ids.index("data-graph"), ids.index("profile-graph"))


# ---- store, slider, figure --------------------------------------------------------------------

class ProfileTest(ProfileCase):
    def test_store_daily(self):
        """Daily: the profile store holds the load's ground temperatures (depths shallow to deep, the loaded days), 1 /daily request in all"""
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        daily, raw = self.measurement_requests()
        self.assertEqual((len(daily), len(raw)), (1, 0))
        self.assertEqual(store["depths"], M13_DEPTHS)
        self.assertEqual(store["labels"], ["-0.10 m", "-0.25 m", "-0.50 m", "-0.75 m"])
        self.assertEqual((store["first"], store["n"]), (day("2026-10-01"), 5))
        self.assertEqual(store["count"][0], [8, 8, 8, 8, 1])

    def test_default_day(self):
        """the slider spans the loaded days, defaults to the last one; the profile shows that day's mean per depth, as the stub's /daily"""
        expected = self.expected_daily(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        slider, style, note = self.setup_profile(store)
        self.assertEqual((slider["min"], slider["max"], slider["value"]), (day("2026-10-01"), day("2026-10-05"), day("2026-10-05")))
        self.assertTrue(slider["marks"])
        self.assertNotEqual(style.get("display"), "none")
        self.assertIsNone(note)
        fig, label, _ = self.draw(slider["value"], "day", store)
        mean = trace(fig, "mean")
        self.assertEqual(mean["y"], [-d for d in M13_DEPTHS])
        for x, sel in zip(mean["x"], expected):
            self.assertAlmostEqual(x, float(sel.series.mean[-1]), delta=0.006)
        self.assertIn("2026-10-05", text_of(label))
        self.assertEqual(fig["layout"]["yaxis"]["range"][1], 0)
        self.assertLess(fig["layout"]["yaxis"]["range"][0], -0.75)

    def test_other_day(self):
        """another day of the slider shows that day's means and the day's min/max band"""
        expected = self.expected_daily(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        fig, label, _ = self.draw(day("2026-10-02"), "day", store)
        mean, band = trace(fig, "mean"), trace(fig, "day-band")
        for x, sel in zip(mean["x"], expected):
            self.assertAlmostEqual(x, float(sel.series.mean[1]), delta=0.006)
        n = len(M13_DEPTHS)                             # band: max down, then min back up
        for i, sel in enumerate(expected):
            self.assertAlmostEqual(band["x"][i], float(sel.series.max[1]), delta=0.006)
            self.assertAlmostEqual(band["x"][2 * n - 1 - i], float(sel.series.min[1]), delta=0.006)
        self.assertEqual(band["fill"], "toself")
        self.assertIn("2026-10-02", text_of(label))

    def test_envelopes(self):
        """Day min/max: band + mean; Annual envelope: annual max/min + mean; Both: all four"""
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        last = day("2026-10-05")
        self.assertEqual(meta(self.draw(last, "day", store)[0]), ["day-band", "mean"])
        self.assertEqual(meta(self.draw(last, "annual", store)[0]), ["annual-max", "annual-min", "mean"])
        self.assertEqual(meta(self.draw(last, "both", store)[0]), ["annual-max", "annual-min", "day-band", "mean"])
        self.assertEqual(meta(self.draw(last, "nonsense", store)[0]), ["day-band", "mean"])

    def test_annual_values(self):
        """the annual envelope is the max and min of the valid daily means up to the day (trumpet's 90 % rule: the 1-measurement last day does not count)"""
        expected = self.expected_daily(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        fig, label, _ = self.draw(day("2026-10-05"), "annual", store)
        hi, lo = trace(fig, "annual-max"), trace(fig, "annual-min")
        self.assertEqual(hi["y"], [-d for d in M13_DEPTHS])
        for i, sel in enumerate(expected):
            valid = sel.series.count >= 0.9 * 8
            self.assertAlmostEqual(hi["x"][i], float(sel.series.mean[valid].max()), delta=0.006)
            self.assertAlmostEqual(lo["x"][i], float(sel.series.mean[valid].min()), delta=0.006)
        text = text_of(label)
        self.assertIn("2026-10-01 to 2026-10-05", text)
        self.assertIn("4 of 5 days", text)

    def test_no_request_on_slider_or_envelope(self):
        """moving the slider or changing the envelope makes no API request"""
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        self.server.requests.clear()
        slider, *_ = self.setup_profile(store)
        for d in range(slider["min"], slider["max"] + 1):
            for env in ("day", "annual", "both"):
                self.draw(d, env, store)
        self.assertEqual(self.server.requests, [])

    def test_day_marker(self):
        """the chosen day is marked on the data figure (one shape patched in, spanning that day)"""
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        _, _, patch = self.draw(day("2026-10-03"), "day", store)
        ops = patch["operations"]
        self.assertEqual([op["location"] for op in ops], [["layout", "shapes"]])
        shape = ops[0]["params"]["value"][0]
        self.assertEqual((shape["x0"], shape["x1"], shape["yref"]),
                         ("2026-10-03 00:00:00", "2026-10-04 00:00:00", "paper"))

    def test_day_clamped(self):
        """a day outside the loaded days (or none) shows the nearest loaded day"""
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        self.assertIn("2026-10-05", text_of(self.draw(None, "day", store)[1]))
        self.assertIn("2026-10-01", text_of(self.draw(day("2020-01-01"), "day", store)[1]))


# ---- Full and zoom ----------------------------------------------------------------------------

class ProfileFullTest(ProfileCase):
    def test_full_requests(self):
        """Full: 1 raw + 1 daily request (the same days), the profile store from the daily; a zoom then makes 1 raw request only"""
        fig, info, message, view, store = self.load_all(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full")
        daily, raw = self.measurement_requests()
        self.assertEqual((len(raw), len(daily)), (1, 1))
        self.assertEqual((raw[0]["start"], raw[0]["end"]), (daily[0]["start"], daily[0]["end"]))
        self.assertEqual(store["depths"], [0.1, 0.25, 0.5])
        self.assertEqual(store["n"], 5)
        self.assertIn(texts.INFO_KIND_PROFILE, text_of(info))
        self.server.requests.clear()
        self.zoom({"xaxis.range[0]": "2026-10-02 00:00:00", "xaxis.range[1]": "2026-10-03 00:00:00"}, view)
        daily, raw = self.measurement_requests()
        self.assertEqual((len(raw), len(daily)), (1, 0))

    def test_full_profile_error(self):
        """Full with a failing profile request: the Full figure is shown, the profile is hidden with the error"""
        error = api_client.ApiError(texts.API_TIMEOUT, "test")
        with mock.patch.object(self.client, "daily", side_effect=error):
            fig, info, message, view, store = self.load_all(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full")
        self.assertIsNone(message)
        self.assertEqual(view["mode"], "full")
        _, style, note = self.setup_profile(store)
        self.assertEqual(style, {"display": "none"})
        self.assertIn(texts.API_TIMEOUT, text_of(note))


# ---- no ground temperatures -------------------------------------------------------------------

class NoGroundTest(ProfileCase):
    def setUp(self):
        super().setUp()
        meta_ = self.server.fixtures["metadata"]
        sensors = {s["id"] for s in meta_["sensors"]
                   if s["recording_device_id"] == 11 and s["name"].startswith("GroundTemp")}
        for ts in meta_["timeseries"]:      # the Campbell of Mast 6 without ground data
            if ts["sensor_id"] in sensors:
                self.server.fixtures["measurements"].pop(ts["id"], None)

    def test_hidden(self):
        """a logger without ground temperatures: the profile body is hidden with a short note, nothing is drawn or marked"""
        fig, info, message, view, store = self.load_all(CAMPBELL_M6, "2026-10-01", "2026-10-05")
        self.assertNotIn(texts.ROW_GROUND, [a["text"] for a in fig["layout"]["annotations"]])
        self.assertEqual(store["keys"], [])
        _, style, note = self.setup_profile(store)
        self.assertEqual(style, {"display": "none"})
        self.assertEqual(note, texts.PROFILE_NO_GROUND)
        fig, label, patch = self.draw(None, "day", store)
        self.assertIsNone(patch)
        self.assertFalse(fig["data"])

    def test_no_store(self):
        """before any load (no store): the profile body is hidden with the empty note"""
        _, style, note = self.setup_profile(None)
        self.assertEqual(style, {"display": "none"})
        self.assertEqual(note, texts.PROFILE_EMPTY)


# ---- profile.py on synthetic data -------------------------------------------------------------

def synthetic(days: int, depths=(0.1, 0.5, 1.0, 2.0, 5.0), first="2024-08-01", count=144):
    """Selected ground temperature series: mean = day index + depth, min/max +- 1."""
    t = np.datetime64(first, "D") + np.arange(days).astype("timedelta64[D]")
    out = []
    for z in depths:
        mean = np.arange(days, dtype=float) * 0.0137 + z
        s = api_client.DailySeries(details={"relative_z": -z}, time=t, mean=mean, min=mean - 1,
                                   max=mean + 1, count=np.full(days, count))
        out.append(api_client.Selected(f"k{z}", f"{-z:+0.2f} m", "C", s))
    return out


class ProfileDataTest(unittest.TestCase):
    def test_annual_window(self):
        """the annual envelope covers exactly the 365 days up to the chosen day"""
        store = profile.store_data(synthetic(500), "x")
        last = profile.last_day(store)
        a = profile.annual_envelope(store, last)
        self.assertEqual((a["days"], a["valid_days"]), (365, 365))
        self.assertEqual(a["end"], profile.day_text(last))
        self.assertEqual(a["start"], profile.day_text(last - 364))
        self.assertAlmostEqual(a["max"][0], round(499 * 0.0137 + 0.1, 2), places=2)
        self.assertAlmostEqual(a["min"][0], round(135 * 0.0137 + 0.1, 2), places=2)
        early = profile.annual_envelope(store, store["first"] + 9)
        self.assertEqual(early["days"], 10)
        self.assertIsNone(profile.annual_envelope(store, last + 1))

    def test_gap_day(self):
        """a day without data inside the record: no profile points, a note in the figure"""
        sel = synthetic(10)
        sel = [api_client.Selected(s.key, s.label, s.unit, api_client.DailySeries(
            details=s.series.details, time=np.delete(s.series.time, 4), mean=np.delete(s.series.mean, 4),
            min=np.delete(s.series.min, 4), max=np.delete(s.series.max, 4),
            count=np.delete(s.series.count, 4))) for s in sel]
        store = profile.store_data(sel, "x")
        self.assertEqual(store["n"], 10)
        p = profile.day_profile(store, store["first"] + 4)
        self.assertEqual(p["depths"], [])
        fig = figures.profile_figure(p, None, "day", store["x_range"], 5.0, "x")
        self.assertEqual(len(fig.data), 0)
        self.assertIn(p["day"], fig.layout.annotations[0].text)

    def test_marks_two_years(self):
        """slider marks over two years: quarter starts, Januaries shown as the year, at most 9 marks"""
        first, last = day("2024-08-15"), day("2026-08-10")
        marks = profile.slider_marks(first, last)
        self.assertLessEqual(len(marks), 9)
        self.assertEqual(marks[day("2025-01-01")], "2025")
        self.assertEqual(marks[day("2025-04-01")], "Apr")
        self.assertTrue(all(first <= k <= last for k in marks))

    def test_marks_short(self):
        """slider marks: months for a year (the first with its year if no January), the two ends for a few days"""
        marks = profile.slider_marks(day("2025-02-10"), day("2025-12-20"))
        self.assertEqual(marks[day("2025-03-01")], "Mar 2025")
        self.assertEqual(len(marks), 10)
        self.assertEqual(profile.slider_marks(day("2026-10-01"), day("2026-10-05")),
                         {day("2026-10-01"): "1 Oct", day("2026-10-05"): "5 Oct"})

    def test_store_size(self):
        """the profile store of 5 depths over two years is small (under 120 KB as JSON)"""
        store = profile.store_data(synthetic(731), "x")
        size = len(json.dumps(store, separators=(",", ":")))
        self.assertLess(size, 120_000, size)

    def test_day_number(self):
        """days are counted from 1970-01-01 (as the slider tooltip in assets/profile.js)"""
        self.assertEqual(profile.day_number("1970-01-02"), 1)
        self.assertEqual(profile.day_number(dt.date(2026, 10, 5)), profile.day_number(np.datetime64("2026-10-05")))
        self.assertEqual(profile.day_text(profile.day_number("2026-10-05")), "2026-10-05")


if __name__ == "__main__":
    unittest.main()
