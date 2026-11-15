"""Unit tests of the ground temperature profile of the phase 2 page (app/v2/profile.py, the
profile part of layout.py, callbacks.py and figures.py, assets/profile.js; docs/plan/PLAN_api-v2.md,
T10, T16): the profile store built by the load callback (per depth and day the mean, min, max
and the annual envelope, checked against the stub and against trumpet.trumpet), the day
slider set by the load, the figure template the browser draws with, the clientside callback
(registered, its JS file served and well-formed), and the API requests each action makes
(slider and envelope: none, they never reach the server; Full: 1 raw + 1 daily; zoom: 1 raw).
The JS itself cannot run here (no JS runtime in the image): it only picks the day's values
from the store, so the tests check the store. Callbacks are driven over HTTP as in
test_v2_page.py, against the stub monitoring_db API (fixtures: tests/fixtures/make_fixtures.py).
Each test's docstring is its name in the test output ("unit: v2_profile: <docstring>")."""
import datetime as dt
import json
import re
import string
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
    def slider(self):
        """The profile slider and body the last load_data set: (slider props, body style, note)."""
        r = self.loaded
        return r["profile-day"], r["profile-body"]["style"], r["profile-note"]["children"]

    def expected_daily(self, logger, start, end):
        """The stub's /daily ground temperatures (via the client; clears the request log)."""
        result = self.client.daily(logger, start, end)
        self.server.requests.clear()
        return api_client.ground_temperature(result.series)


# ---- layout and the clientside callback ------------------------------------------------------

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
        """the day slider updates while dragged, shows its date in a tooltip (assets/profile.js); the profile body starts hidden"""
        slider = self.by_id["profile-day"]
        self.assertEqual(slider.updatemode, "drag")
        self.assertEqual(slider.tooltip["transform"], "nukiDay")
        self.assertFalse(slider.allow_direct_input)
        self.assertEqual(self.by_id["profile-body"].style, {"display": "none"})

    def test_profile_after_figure(self):
        """the profile card comes after the data figure"""
        ids = [getattr(c, "id", None) for c in app.v2_app.layout._traverse()]
        self.assertLess(ids.index("data-graph"), ids.index("profile-graph"))

    def test_clientside_callback(self):
        """the profile is drawn by the clientside function dash_clientside.nuki.profile (day, envelope, store -> profile figure, label, data figure patch); no server callback takes the profile store"""
        clientside = [cb for cb in app.v2_app._callback_list if cb.get("clientside_function")]
        self.assertEqual(len(clientside), 1)
        cb = clientside[0]
        self.assertEqual(cb["clientside_function"], {"namespace": "nuki", "function_name": "profile"})
        self.assertEqual([(i["id"], i["property"]) for i in cb["inputs"]],
                         [("profile-day", "value"), ("profile-envelope", "value"), ("profile", "data")])
        outputs = [re.sub(r"@[0-9a-f]+$", "", o) for o in cb["output"].strip(".").split("...")]
        self.assertEqual(outputs, ["profile-graph.figure", "profile-label.children", "data-graph.figure"])
        for key, server_cb in app.v2_app.callback_map.items():
            if "callback" not in server_cb:
                continue
            used = [d["id"] for d in server_cb.get("inputs", []) + server_cb.get("state", [])]
            self.assertNotIn("profile", used, key)

    def test_js_file(self):
        """assets/profile.js is served, defines nukiDay and dash_clientside.nuki.profile, and its brackets balance (outside strings and comments)"""
        r = app.server.test_client().get("/app/nuki/v2/assets/profile.js")
        self.assertEqual(r.status_code, 200)
        js = r.get_data(as_text=True)
        self.assertIn("dccFunctions.nukiDay", js)
        self.assertIn("window.dash_clientside.nuki", js)
        self.assertRegex(js, r"\bprofile: function \(dayValue, envelope, store\)")
        code = re.sub(r"/\*.*?\*/", "", js, flags=re.S)
        code = re.sub(r"//[^\n]*", "", code)
        code = re.sub(r'"(?:\\.|[^"\\])*"' + r"|'(?:\\.|[^'\\])*'", '""', code)
        stack, pairs = [], {")": "(", "]": "[", "}": "{"}
        for ch in code:
            if ch in "([{":
                stack.append(ch)
            elif ch in pairs:
                self.assertTrue(stack and stack.pop() == pairs[ch], "unbalanced " + ch)
        self.assertEqual(stack, [])
        # the store fields the JS reads are the ones store_data/profile_callbacks write
        for name in ("first", "n", "keys", "depths", "labels", "mean", "min", "max", "amax", "amin", "valid",
                     "window", "draw", "shapes", "day_station", "station_list", "stations", "source"):
            self.assertRegex(js, rf"store\.{name}\b|\"{name}\"", name)


# ---- store, slider ----------------------------------------------------------------------------

class ProfileTest(ProfileCase):
    def test_store_daily(self):
        """Daily: the profile store holds the load's ground temperatures (depths shallow to deep, the loaded days, mean/min/max as the stub's /daily), 1 /daily request in all"""
        expected = self.expected_daily(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        daily, raw = self.measurement_requests()
        self.assertEqual((len(daily), len(raw)), (1, 0))
        self.assertEqual(store["depths"], M13_DEPTHS)
        self.assertEqual(store["labels"], ["-0.10 m", "-0.25 m", "-0.50 m", "-0.75 m"])
        self.assertEqual((store["first"], store["n"]), (day("2026-10-01"), 5))
        self.assertEqual(store["depth_max"], 0.75)
        for r, sel in enumerate(expected):
            for name in ("mean", "min", "max"):
                self.assertEqual(len(store[name][r]), 5)
                for got, want in zip(store[name][r], getattr(sel.series, name)):
                    self.assertAlmostEqual(got, float(want), delta=0.006)

    def test_slider_from_load(self):
        """the load sets the slider: the loaded days, default the last one, marks; the profile body shown"""
        self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        slider, style, note = self.slider()
        self.assertEqual((slider["min"], slider["max"], slider["value"]), (day("2026-10-01"), day("2026-10-05"), day("2026-10-05")))
        self.assertTrue(slider["marks"])
        self.assertNotEqual(style.get("display"), "none")
        self.assertIsNone(note)

    def test_annual_values(self):
        """the annual envelope of each day: max and min of the valid daily means up to it (trumpet's 90 % rule: the 1-measurement last day does not count), and its valid days"""
        expected = self.expected_daily(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        for r, sel in enumerate(expected):
            valid = sel.series.count >= 0.9 * 8
            self.assertAlmostEqual(store["amax"][r][4], float(sel.series.mean[valid].max()), delta=0.006)
            self.assertAlmostEqual(store["amin"][r][4], float(sel.series.mean[valid].min()), delta=0.006)
            self.assertAlmostEqual(store["amax"][r][0], float(sel.series.mean[0]), delta=0.006)
        self.assertEqual(store["valid"], [1, 2, 3, 4, 4])
        self.assertEqual(store["window"], profile.WINDOW_DAYS)

    def test_template(self):
        """the figure template: traces per envelope choice (Day min/max: band + mean; Annual: max/min + mean; Both: all four), styles and axes fixed by the store"""
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        draw = store["draw"]
        self.assertEqual(draw["envelopes"], {"day": ["day-band", "mean"],
                                             "annual": ["annual-max", "annual-min", "mean"],
                                             "both": ["annual-max", "annual-min", "day-band", "mean"]})
        traces = draw["traces"]
        self.assertEqual({k: t["meta"] for k, t in traces.items()}, {k: k for k in traces})
        self.assertEqual(traces["day-band"]["fill"], "toself")
        self.assertEqual({traces["annual-max"]["line"]["dash"], traces["annual-min"]["line"]["dash"]}, {"dash"})
        self.assertTrue(all("x" not in t and "y" not in t for t in traces.values()))
        layout = draw["layout"]
        self.assertNotIn("template", layout)        # a plotly.py name, unknown to plotly.js
        self.assertEqual(layout["yaxis"]["range"][1], 0)
        self.assertAlmostEqual(layout["yaxis"]["range"][0], -1.08 * 0.75)
        lo, hi = store["x_range"]
        self.assertLess(layout["xaxis"]["range"][0], lo)
        self.assertGreater(layout["xaxis"]["range"][1], hi)
        self.assertEqual(layout["uirevision"], CAMPBELL_M13)
        self.assertEqual(layout["height"], figures.PROFILE_HEIGHT)
        json.dumps(draw)                            # plain JSON for the browser

    def test_label_texts(self):
        """the label templates are texts.py's, with only the fields the JS fills (day, station, start, end, valid, days)"""
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        t = store["draw"]["texts"]
        self.assertEqual(t["label"], texts.PROFILE_LABEL)
        self.assertEqual(t["annual"], "; " + texts.PROFILE_LABEL_ANNUAL)
        self.assertEqual((t["short"], t["station"]), (texts.PROFILE_LABEL_SHORT, texts.PROFILE_LABEL_STATION))
        fields = {f for text in t.values() for _, f, spec, _ in string.Formatter().parse(text) if f}
        fields.add(next(f for _, f, _, _ in string.Formatter().parse(store["draw"]["no_day"]["text"]) if f))
        self.assertLessEqual(fields, {"day", "station", "start", "end", "valid", "days"})
        label = (t["label"].format(day="2026-10-05")
                 + t["annual"].format(start="2026-10-01", end="2026-10-05", valid=4, days=5)
                 + t["short"].format(start="2026-10-01"))
        self.assertIn("4 of 5 days", label)

    def test_day_marker(self):
        """the day marker template: a rectangle over every row of the data figure (paper height); the JS sets x0/x1 to the day's start and end; the store has no change lines for one logger"""
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        marker = store["draw"]["marker"]
        self.assertEqual((marker["type"], marker["xref"], marker["yref"], marker["y0"], marker["y1"]),
                         ("rect", "x", "paper", 0, 1))
        self.assertNotIn("x0", marker)
        self.assertEqual(store["shapes"], [])

    def test_no_request_after_load(self):
        """after a load the slider and envelope need nothing from the server: the store holds every day (no day outside the arrays), and no server callback is triggered by them"""
        *_, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05")
        for name in ("mean", "min", "max", "amax", "amin"):
            self.assertEqual({len(row) for row in store[name]}, {store["n"]})
        self.assertEqual(len(store["valid"]), store["n"])
        for key, cb in self.dash.callback_map.items():
            if "callback" in cb:
                ids = {d["id"] for d in cb.get("inputs", [])}
                self.assertFalse(ids & {"profile-day", "profile-envelope", "profile"}, key)


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
        _, style, note = self.slider()
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
        """a logger without ground temperatures: the profile body is hidden with a short note, nothing to draw (no template)"""
        fig, info, message, view, store = self.load_all(CAMPBELL_M6, "2026-10-01", "2026-10-05")
        self.assertNotIn(texts.ROW_GROUND, [a["text"].split(",")[0] for a in fig["layout"]["annotations"]])
        self.assertEqual(store, {"logger": CAMPBELL_M6, "keys": []})
        _, style, note = self.slider()
        self.assertEqual(style, {"display": "none"})
        self.assertEqual(note, texts.PROFILE_NO_GROUND)

    def test_no_store(self):
        """no logger (no store): the profile body is hidden with the empty note"""
        self.load_all(None, None, None)
        _, style, note = self.slider()
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


def irregular(days: int, depths=(0.1, 0.5, 1.0, 2.0), seed=1):
    """Selected series with noise, gaps (days left out) and changing daily counts (144 most
    days, a stretch of 48 per day (another logger setting), single short days), so that the
    90 % rule and the window's most frequent count matter."""
    rng = np.random.default_rng(seed)
    t = np.datetime64("2023-08-01", "D") + np.arange(days).astype("timedelta64[D]")
    out = []
    for k, z in enumerate(depths):
        mean = 6 * np.sin(np.arange(days) / 58.0) / (1 + z) + rng.normal(0, 0.4, days) - 2
        count = np.full(days, 144)
        count[300:520] = 48
        count[rng.random(days) < 0.06] = 100            # short days (< 90 % of 144, >= of 48)
        count[rng.random(days) < 0.03] = 20
        keep = rng.random(days) > 0.05
        keep[200 + k * 3:230 + k * 3] = False           # a gap, a little different per depth
        s = api_client.DailySeries(details={"relative_z": -z}, time=t[keep], mean=mean[keep],
                                   min=mean[keep] - 1.5, max=mean[keep] + 1.5, count=count[keep])
        out.append(api_client.Selected(f"k{z}", f"{-z:+0.2f} m", "C", s))
    return out


def assert_envelope_as_trumpet(test, store, m, i, rows=None):
    """The store's annual envelope of day index i equals profile.trumpet_envelope (trumpet.py)."""
    ref = profile.trumpet_envelope(m, i, rows)
    rows = list(range(len(m.keys))) if rows is None else list(rows)
    for r in rows:
        if r in ref["rows"]:
            k = ref["rows"].index(r)
            test.assertEqual((store["amax"][r][i], store["amin"][r][i]),
                             (round(ref["max"][k], 2), round(ref["min"][k], 2)), (i, r))
        else:
            test.assertEqual((store["amax"][r][i], store["amin"][r][i]), (None, None), (i, r))
    test.assertEqual(store["valid"][i], ref["valid_days"], i)


class ProfileDataTest(unittest.TestCase):
    def test_annual_as_trumpet(self):
        """the rolling annual envelope equals trumpet.trumpet on the 365-day window, day by day (record start, gaps, a change of the daily count, the end)"""
        sel = irregular(800)
        store = profile.store_data(sel, "x")
        m = profile.matrices(sel)
        for i in (0, 1, 9, 150, 205, 229, 299, 300, 364, 365, 366, 420, 519, 520, 600, 664, 665, 799):
            assert_envelope_as_trumpet(self, store, m, i)

    def test_annual_window(self):
        """the annual envelope covers exactly the 365 days up to the chosen day (fewer at the record's start)"""
        store = profile.store_data(synthetic(500), "x")
        self.assertEqual(store["valid"][-1], 365)
        self.assertAlmostEqual(store["amax"][0][-1], round(499 * 0.0137 + 0.1, 2), places=2)
        self.assertAlmostEqual(store["amin"][0][-1], round(135 * 0.0137 + 0.1, 2), places=2)
        self.assertEqual(store["valid"][9], 10)
        self.assertAlmostEqual(store["amin"][0][9], 0.1, places=2)

    def test_gap_day(self):
        """a day without data inside the record: None for every depth that day (the JS then shows the no-data note), the envelope carries on"""
        sel = synthetic(10)
        sel = [api_client.Selected(s.key, s.label, s.unit, api_client.DailySeries(
            details=s.series.details, time=np.delete(s.series.time, 4), mean=np.delete(s.series.mean, 4),
            min=np.delete(s.series.min, 4), max=np.delete(s.series.max, 4),
            count=np.delete(s.series.count, 4))) for s in sel]
        store = profile.store_data(sel, "x")
        self.assertEqual(store["n"], 10)
        self.assertEqual({row[4] for row in store["mean"]}, {None})
        self.assertNotIn(None, [row[4] for row in store["amax"]])
        self.assertEqual(store["valid"][4], 4)

    def test_min_max_fallback(self):
        """a day without min/max from the API: the store's min and max are the mean"""
        sel = synthetic(3)
        s = sel[0].series
        sel[0] = api_client.Selected(sel[0].key, sel[0].label, "C", api_client.DailySeries(
            details=s.details, time=s.time, mean=s.mean, min=np.full(3, np.nan), max=np.full(3, np.nan), count=s.count))
        store = profile.store_data(sel, "x")
        self.assertEqual(store["min"][0], store["mean"][0])
        self.assertEqual(store["max"][0], store["mean"][0])

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
        """the profile store over two years is small: under 125 KB as JSON for 5 depths, under 240 KB for 10 (with the figure template)"""
        for n_depths, limit in ((5, 125_000), (10, 240_000)):
            sel = synthetic(731, depths=[0.1 * (k + 1) for k in range(n_depths)])
            store = profile.store_data(sel, "x")
            store["draw"] = figures.profile_spec(store, "x")
            size = len(json.dumps(store, separators=(",", ":")))
            self.assertLess(size, limit, (n_depths, size))

    def test_day_number(self):
        """days are counted from 1970-01-01 (as the slider tooltip in assets/profile.js)"""
        self.assertEqual(profile.day_number("1970-01-02"), 1)
        self.assertEqual(profile.day_number(dt.date(2026, 10, 5)), profile.day_number(np.datetime64("2026-10-05")))
        self.assertEqual(profile.day_text(profile.day_number("2026-10-05")), "2026-10-05")


if __name__ == "__main__":
    unittest.main()
