"""Unit tests of the phase 2 page (app/v2/: layout, callbacks, figures; docs/plan/PLAN_api-v2.md,
T9): the layout's ids and defaults, the callbacks driven over HTTP with Flask's test client
(POST <prefix>_dash-update-component, as the browser does) against the stub monitoring_db
API, the requests each action makes, the figure rows, and that the page loads no CDN
resource and keeps its CSS to itself. The fixture data is described in
tests/fixtures/make_fixtures.py. Each test's docstring is its name in the test output
("unit: v2_page: <docstring>")."""
import datetime as dt
import hashlib
import json
import re
import unittest
from unittest import mock

from flask import Flask

from support import APP_DIR, ApiTestCase, config, fake_api

import app  # noqa: E402  (the production server with both pages)
import v2  # noqa: E402
from v2 import api_client, callbacks, figures, texts  # noqa: E402
from v2.api_client import Client  # noqa: E402

TEKBOX_M6 = "deveui:feedbeefcafe0002"          # device 10, 2026-09-01..05
CAMPBELL_M6 = "serial:SIS2024-01:12096"        # device 11, 2026-10-01..05
CAMPBELL_M13 = "serial:SIS2024-05:12216"       # device 41, 2026-10-01..05, 4 depths
SISIMIUT_TEST = "serial:SIS2024-07:12096"      # device 60: battery and ground temperature only
BOOTSTRAP_SHA256 = "d85327d99c7a3ee1f9b5d0500d1370acea3ad2db39c163c2f51f232baedbdede"   # as webapp_grl_tsp

CAMPBELL_ROWS = [texts.ROW_GROUND, texts.ROW_AIR, texts.ROW_HUMIDITY, texts.ROW_PRESSURE,
                 "Inclination XAngle", "Inclination YAngle", texts.ROW_SENSOR_TEMPERATURE,
                 texts.ROW_BATTERY]
TEKBOX_ROWS = [texts.ROW_GROUND, texts.ROW_AIR, texts.ROW_HUMIDITY, texts.ROW_PRESSURE,
               "Inclination PitchAngle", "Inclination RollAngle", texts.ROW_HEADING,
               texts.ROW_SENSOR_TEMPERATURE, texts.ROW_BATTERY]


def components(node):
    if isinstance(node, list):
        for item in node:
            yield from components(item)
    elif isinstance(node, dict):
        if "props" in node:
            yield node
            for value in node["props"].values():
                yield from components(value)
        else:
            for value in node.values():
                yield from components(value)


def text_of(node) -> str:
    """All strings in a component tree (JSON), joined."""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return " ".join(text_of(n) for n in node)
    if isinstance(node, dict):
        return text_of(node.get("props", {}).get("children"))
    return ""


def row_titles(fig):
    """The annotation texts, row titles without what they show (", daily mean", ...)."""
    return [a["text"].split(", ")[0] for a in fig["layout"].get("annotations", [])]


def full_titles(fig):
    """The row titles as shown (with what the row shows)."""
    return [a["text"] for a in fig["layout"].get("annotations", []) if str(a.get("name", "")).startswith("row:")]


PROFILE_OUTPUTS = [("profile-day", "min"), ("profile-day", "max"), ("profile-day", "marks"),
                   ("profile-day", "value"), ("profile-body", "style"), ("profile-note", "children")]


class PageTestCase(ApiTestCase):
    """A fresh v2 Dash app on its own Flask server per test, with a client on the stub."""

    def setUp(self):
        super().setUp()
        self.set_settings(backend_api_daily_chunk_years=3)
        self.client = Client.from_settings(config.settings)
        self.dash = v2.create_app(Flask("v2test"), client=self.client)
        self.http = self.dash.server.test_client()

    def call(self, outputs, inputs, state=(), changed=None):
        """POST one callback, as the browser: outputs [(id, prop)], inputs/state [(id, prop,
        value)]. Returns {id: {prop: value}} (empty when the callback changed nothing)."""
        wanted = [f"{i}.{p}" for i, p in outputs]
        key = next(k for k in self.dash.callback_map
                   if [re.sub(r"@[0-9a-f]+$", "", o) for o in k.strip(".").split("...")] == wanted)
        props = [o.rsplit(".", 1) for o in key.strip(".").split("...")]
        payload = {
            "output": key,
            "outputs": [{"id": i, "property": p} for i, p in props],
            "inputs": [{"id": i, "property": p, "value": v} for i, p, v in inputs],
            "changedPropIds": [changed or f"{inputs[0][0]}.{inputs[0][1]}"],
            "state": [{"id": i, "property": p, "value": v} for i, p, v in state],
        }
        r = self.http.post(v2.URL_PREFIX + "_dash-update-component", json=payload)
        if r.status_code == 204:           # every output no_update (PreventUpdate)
            return {}
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:500])
        return r.get_json()["response"]

    def masts(self):
        return self.call([("masts", "data"), ("mast", "options"), ("mast", "value"),
                          ("masts-message", "children")], [("url", "pathname", v2.URL_PREFIX)])

    def load_all(self, logger, start, end, resolution="daily", masts=None, view=None, changed=None):
        """load_data: (figure, info children, message children, view, profile store); the
        whole answer (also the profile slider and body) in self.loaded. ``masts``: the
        catalogue store (needed for All loggers only); ``view``: the view store before (the
        Daily/Full switch, ``changed="resolution.value"``, keeps its window)."""
        r = self.call([("data-graph", "figure"), ("info", "children"), ("message", "children"),
                       ("view", "data"), ("profile", "data")] + PROFILE_OUTPUTS + [("data-heading", "children")],
                      [("logger", "value", logger), ("period", "start_date", start),
                       ("period", "end_date", end), ("resolution", "value", resolution)],
                      state=[("masts", "data", masts), ("view", "data", view)], changed=changed)
        self.loaded = r
        return (r["data-graph"]["figure"], r["info"]["children"], r["message"]["children"],
                r["view"]["data"], r["profile"]["data"])

    def load(self, logger, start, end, resolution="daily"):
        """load_data: (figure, info children, message children, view)."""
        return self.load_all(logger, start, end, resolution)[:4]

    def zoom(self, relayout, view):
        return self.call([("data-graph", "figure"), ("info", "children"), ("message", "children"),
                          ("view", "data"), ("data-heading", "children")],
                         [("data-graph", "relayoutData", relayout)], state=[("view", "data", view)])

    def measurement_requests(self):
        return self.api_requests("measurements/daily"), self.api_requests("measurements/raw")


# ---- layout -----------------------------------------------------------------------------------

class LayoutTest(unittest.TestCase):
    def setUp(self):
        self.by_id = {c.id: c for c in app.v2_app.layout._traverse() if getattr(c, "id", None)}

    def test_ids(self):
        """the v2 layout has the selectors, the store, the info line, the message and the figure"""
        for cid in ("classic-link", "url", "masts", "masts-message", "mast", "logger", "period",
                    "resolution", "message", "info", "data-graph", "view", "profile", "profile-note",
                    "profile-body", "profile-day", "profile-envelope", "profile-label", "profile-graph",
                    "profile-card", "download-card", "data-heading"):
            self.assertIn(cid, self.by_id)

    def test_cards_side_by_side(self):
        """the profile (lg=8) and the download card (lg=4) sit side by side on lg+ screens, top-aligned, not stretched; full width (stacked) below lg"""
        rows = [c for c in app.v2_app.layout._traverse() if type(c).__name__ == "Row"
                and {getattr(getattr(col, "children", None), "id", None) for col in c.children
                     if type(col).__name__ == "Col"} == {"profile-card", "download-card"}]
        self.assertEqual(len(rows), 1)
        cols = rows[0].children
        self.assertEqual([col.children.id for col in cols], ["profile-card", "download-card"])
        self.assertEqual([(col.xs, col.lg) for col in cols], [(12, 8), (12, 4)])
        self.assertIn("align-items-start", rows[0].className)
        for col in cols:
            self.assertNotIn("h-100", col.children.className)
        # download card: stacked top to bottom, the button directly after the logger checklist
        ids = [getattr(c, "id", None) for c in self.by_id["download-card"]._traverse()]
        order = ["download-from", "download-to", "download-all", "download-loggers", "download-button",
                 "download-message"]
        self.assertEqual([i for i in ids if i in order], order)
        # the profile graph keeps its explicit height (it is created hidden, 5276c98)
        self.assertEqual(self.by_id["profile-graph"].style, {"height": f"{figures.PROFILE_HEIGHT}px"})

    def test_defaults(self):
        """resolution defaults to Daily and persists per session; the date picker has no persistence"""
        res = self.by_id["resolution"]
        self.assertEqual(res.value, "daily")
        self.assertEqual([o["value"] for o in res.options], ["daily", "full"])
        self.assertTrue(res.persistence)
        self.assertEqual(res.persistence_type, "session")
        self.assertFalse(getattr(self.by_id["period"], "persistence", None))
        self.assertEqual(self.by_id["period"].updatemode, "bothdates")

    def test_figure_in_loading(self):
        """the figure is inside a dcc.Loading"""
        loadings = [c for c in app.v2_app.layout._traverse() if type(c).__name__ == "Loading"]
        graphs = [g for lo in loadings for g in lo._traverse() if getattr(g, "id", None) == "data-graph"]
        self.assertEqual(len(graphs), 1)


# ---- assets and CDN ---------------------------------------------------------------------------

class AssetsTest(unittest.TestCase):
    def setUp(self):
        self.http = app.server.test_client()

    def get(self, path):
        r = self.http.get(path)
        return r.status_code, r.get_data()

    def test_bootstrap_file(self):
        """app/v2/assets/bootstrap.min.css is byte-identical to webapp_grl_tsp's (sha256) and Bootstrap 5.3.8"""
        data = (APP_DIR / "v2" / "assets" / "bootstrap.min.css").read_bytes()
        self.assertEqual(hashlib.sha256(data).hexdigest(), BOOTSTRAP_SHA256)
        self.assertIn(b"Bootstrap  v5.3.8", data[:200])

    def test_v2_serves_its_assets(self):
        """GET /app/nuki/v2/ links its bootstrap.min.css and style.css, served from app/v2/assets/"""
        status, body = self.get(v2.URL_PREFIX)
        self.assertEqual(status, 200)
        html_ = body.decode()
        for name in ("bootstrap.min.css", "style.css"):
            self.assertRegex(html_, rf'href="/app/nuki/v2/assets/{re.escape(name)}(\?m=[0-9.]+)?"')
        status, css = self.get(v2.URL_PREFIX + "assets/bootstrap.min.css")
        self.assertEqual(status, 200)
        self.assertEqual(hashlib.sha256(css).hexdigest(), BOOTSTRAP_SHA256)
        self.assertTrue(app.v2_app.config.serve_locally)

    def test_no_cdn(self):
        """the v2 index page and its Dash config load no script or CSS from an external http(s) URL"""
        status, body = self.get(v2.URL_PREFIX)
        self.assertEqual(status, 200)
        html_ = body.decode()
        external = re.findall(r'(?:src|href)\s*=\s*"((?:https?:)?//[^"]*)"', html_, re.I)
        self.assertEqual(external, [])
        m = re.search(r'<script id="_dash-config" type="application/json">(.*?)</script>', html_, re.S)
        self.assertNotRegex(m.group(1), r"https?://")
        self.assertEqual(app.v2_app.config.external_stylesheets, [])
        self.assertEqual(app.v2_app.config.external_scripts, [])

    def test_classic_without_v2_css(self):
        """the classic page /app/nuki/ loads neither the v2 CSS nor any assets file"""
        status, body = self.get("/app/nuki/")
        self.assertEqual(status, 200)
        html_ = body.decode()
        self.assertNotIn("bootstrap.min.css", html_)
        self.assertNotIn("style.css", html_)
        self.assertNotIn("/assets/", html_)
        self.assertNotEqual(app.app.config.assets_folder, app.v2_app.config.assets_folder)


# ---- selectors --------------------------------------------------------------------------------

class SelectorTest(PageTestCase):
    def test_masts(self):
        """page load: the masts from the catalogue (Mast 6, 9, 13), the first one chosen, no message"""
        r = self.masts()
        self.assertEqual([o["value"] for o in r["mast"]["options"]], ["Mast 6", "Mast 9", "Mast 13"])
        self.assertEqual(r["mast"]["value"], "Mast 6")
        self.assertIsNone(r["masts-message"]["children"])

    def test_masts_error(self):
        """page load with the API down: an alert with the ApiError text, no masts"""
        self.server.force = (500, {"error": {"code": "INTERNAL_ERROR", "message": "boom", "details": {}}})
        r = self.masts()
        self.assertEqual(r["mast"]["options"], [])
        self.assertIn("boom", text_of(r["masts-message"]["children"]))

    def test_loggers_default_latest(self):
        """mast -> its loggers, default the logger with the latest data (Mast 6: the Campbell; Mast 13: CR300_M13 only)"""
        masts = self.masts()["masts"]["data"]

        def loggers(name):
            r = self.call([("logger", "options"), ("logger", "value")], [("mast", "value", name)],
                          state=[("masts", "data", masts)])
            return [o["value"] for o in r["logger"]["options"]], r["logger"]["value"]
        options, value = loggers("Mast 6")
        self.assertEqual(sorted(options[:-1]), sorted([TEKBOX_M6, CAMPBELL_M6]))
        self.assertEqual(options[-1], "all:Mast 6")          # All loggers (T11), last
        self.assertEqual(value, CAMPBELL_M6)
        self.assertEqual(loggers("Mast 13"), ([CAMPBELL_M13], CAMPBELL_M13))

    def test_period_from_logger(self):
        """logger -> date picker limits and range = the logger's first..last measurement day"""
        masts = self.masts()["masts"]["data"]
        props = ["min_date_allowed", "max_date_allowed", "start_date", "end_date", "initial_visible_month"]
        r = self.call([("period", p) for p in props], [("logger", "value", TEKBOX_M6)],
                      state=[("masts", "data", masts)])
        self.assertEqual([r["period"][p] for p in props],
                         ["2026-09-01", "2026-09-05", "2026-09-01", "2026-09-05", "2026-09-05"])


# ---- Daily and Full ---------------------------------------------------------------------------

class DataTest(PageTestCase):
    def test_daily_one_request(self):
        """Daily: exactly 1 /daily request for the period, no /raw; one line of daily means per series, no min/max band"""
        fig, info, message, view = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05")
        daily, raw = self.measurement_requests()
        self.assertEqual([(p["start"], p["end"]) for p in daily],
                         [("2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertEqual(raw, [])
        self.assertIsNone(message)
        self.assertEqual(view["mode"], "daily")
        self.assertFalse([t for t in fig["data"] if t.get("fill")])
        self.assertEqual(len(fig["data"]), 3 + 3 + 2 + 1 + 1)   # 3 depths, 3 weather, 2 angles, sensor temp, battery
        self.assertEqual([t.get("meta") for t in fig["data"]], view["keys"])
        self.assertTrue(all("customdata" in t for t in fig["data"]))     # hover: the day's min and max
        self.assertIn("1 request(s)", text_of(info))
        self.assertIn(texts.INFO_DAILY.format(start="2026-10-01", end="2026-10-05"), text_of(info))
        self.assertNotIn("minimum", text_of(info))

    def test_daily_gaps(self):
        """Daily: a line breaks at a missing day (a None point), it is not drawn across the gap"""
        result = self.client.daily(CAMPBELL_M6, "2026-10-01", "2026-10-05")
        sel = next(iter(result.series.values()))
        keep = [0, 1, 3, 4]
        gap = api_client.DailySeries(details=sel.details, time=sel.time[keep], mean=sel.mean[keep],
                                     min=sel.min[keep], max=sel.max[keep], count=sel.count[keep])
        d = figures.daily_trace_data(gap)
        self.assertEqual(len(d["x"]), 5)
        self.assertIsNone(d["mean"][2])

    def test_row_titles_say_what(self):
        """row titles say what is shown: "..., daily mean" (Daily), "..., all measurements" or "..., thinned (min/max per ~...)" (Full)"""
        fig, *_ = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05")
        self.assertEqual(full_titles(fig), [texts.ROW_DAILY.format(title=t) for t in CAMPBELL_ROWS])
        self.assertEqual(full_titles(fig)[0], "Ground temperature, daily mean")
        fig, *_ = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full")
        self.assertEqual(full_titles(fig), [texts.ROW_FULL.format(title=t) for t in CAMPBELL_ROWS])
        with mock.patch.object(figures, "MAX_POINTS", 10):
            fig, *_ = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full")
        self.assertRegex(full_titles(fig)[0], r"^Ground temperature, thinned \(min/max per ~\d+ h\)$")

    def test_tick_labels_every_row(self):
        """every row shows its own date tick labels; the x axes stay linked (matched to one axis); room between the rows for the labels and the next title"""
        for resolution in ("daily", "full"):
            fig, *_ = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05", resolution)
            lay = fig["layout"]
            n = len(CAMPBELL_ROWS)
            axes = [k for k in lay if re.fullmatch(r"xaxis\d*", k)]
            self.assertEqual(len(axes), n)
            self.assertTrue(all(lay[a].get("showticklabels") is True for a in axes), resolution)
            self.assertEqual({lay["xaxis" + (str(i) if i > 1 else "")].get("matches") for i in range(1, n)}, {f"x{n}"})
            plot_px = lay["height"] - lay["margin"]["t"] - lay["margin"]["b"]
            for i in range(1, n):
                upper, lower = lay["yaxis" + (str(i) if i > 1 else "")]["domain"], lay[f"yaxis{i + 1}"]["domain"]
                self.assertGreaterEqual((upper[0] - lower[1]) * plot_px, 75)

    def test_rows_campbell(self):
        """figure rows of a Campbell logger: ground temperature (taller), weather, pressure in kPa, one row per angle, sensor temperature, battery; no heading"""
        fig, *_ = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05")
        self.assertEqual(row_titles(fig), CAMPBELL_ROWS)
        lay = fig["layout"]
        ground = lay["yaxis"]["domain"]
        battery = lay[f"yaxis{len(CAMPBELL_ROWS)}"]["domain"]
        self.assertGreater(ground[1] - ground[0], 1.5 * (battery[1] - battery[0]))
        n = len(CAMPBELL_ROWS)                  # shared x axes: all match the bottom one
        self.assertEqual({lay["xaxis" + (str(i) if i > 1 else "")].get("matches") for i in range(1, n)}, {f"x{n}"})
        self.assertEqual(lay["yaxis4"]["title"]["text"], "kPa")
        pressure = next(t for t in fig["data"] if t.get("yaxis") == "y4" and "customdata" in t)
        self.assertAlmostEqual(pressure["y"][0], 101.3, delta=0.2)
        names = [t["name"] for t in fig["data"] if t.get("yaxis") in (None, "y") and "customdata" in t]
        self.assertEqual(names, ["-0.10 m", "-0.25 m", "-0.50 m"])

    def test_rows_tekbox(self):
        """figure rows of a Tekbox logger include Pitch, Roll (own rows) and the compass heading"""
        fig, *_ = self.load(TEKBOX_M6, "2026-09-01", "2026-09-05")
        self.assertEqual(row_titles(fig), TEKBOX_ROWS)

    def test_rows_absent(self):
        """rows without data are left out (a logger with only battery and ground temperature: 2 rows)"""
        fig, *_ = self.load(SISIMIUT_TEST, "2026-10-01", "2026-10-02")
        self.assertEqual(row_titles(fig), [texts.ROW_GROUND, texts.ROW_BATTERY])

    def test_no_logger(self):
        """no logger: an empty figure and no API request"""
        fig, info, message, view = self.load(None, None, None)
        self.assertEqual(self.server.requests, [])
        self.assertIsNone(view)
        self.assertEqual([a["text"] for a in fig["layout"]["annotations"]], [texts.FIGURE_EMPTY])

    def test_no_data(self):
        """a period without data: an info message, no rows"""
        fig, info, message, view = self.load(CAMPBELL_M6, "2025-01-01", "2025-01-10")
        self.assertIn(texts.NO_DATA.format(start="2025-01-01", end="2025-01-10"), text_of(message))
        self.assertIsNone(view)

    def test_full_one_request(self):
        """Full: exactly 1 /raw request for the period and 1 /daily for the profile (same days); one line per series with its column key"""
        fig, info, message, view = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full")
        daily, raw = self.measurement_requests()
        self.assertEqual([(p["start"], p["end"]) for p in raw],
                         [("2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertEqual([(p["start"], p["end"]) for p in daily],
                         [("2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertIn("2 request(s)", text_of(info))
        self.assertIsNone(message)
        self.assertEqual(row_titles(fig), CAMPBELL_ROWS)
        self.assertEqual(view["mode"], "full")
        self.assertEqual(view["range"], ["2026-10-01T00:00:00+00:00", "2026-10-05T23:59:59+00:00"])
        self.assertEqual([t.get("meta") for t in fig["data"]], view["keys"])
        self.assertTrue(all(view["keys"]))
        self.assertFalse([t for t in fig["data"] if t.get("fill")])
        self.assertIn(texts.INFO_FULL.format(start="2026-10-01", end="2026-10-05"), text_of(info))

    def test_full_limit(self):
        """Full over more than a year loads, in one raw request, the year up to the end date (the profile's daily the same year), and the info line says so"""
        fig, info, message, view = self.load(CAMPBELL_M6, "2024-01-01", "2026-10-05", "full")
        daily, raw = self.measurement_requests()
        self.assertEqual([(p["start"], p["end"]) for p in raw],
                         [("2025-10-06T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertEqual([(p["start"], p["end"]) for p in daily],
                         [("2025-10-06T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertEqual(view["range"], ["2025-10-06T00:00:00+00:00", "2026-10-05T23:59:59+00:00"])
        self.assertIn(texts.INFO_FULL_LIMIT, text_of(info))
        lo, hi, limited = callbacks.full_period(dt.date(2025, 10, 6), dt.date(2026, 10, 5))
        self.assertEqual((lo, limited), (dt.date(2025, 10, 6), False))       # exactly one year
        lo, hi, limited = callbacks.full_period(dt.date(2027, 1, 1), dt.date(2028, 2, 28))
        self.assertEqual((lo, limited), (dt.date(2027, 3, 1), True))         # 29 Feb 2027 does not exist
        lo, hi, limited = callbacks.full_period(dt.date(2023, 1, 1), dt.date(2024, 6, 30))
        self.assertEqual((lo, limited), (dt.date(2023, 7, 1), True))         # 366 days (leap year)

    def test_full_thinned(self):
        """Full thins dense series to about MAX_POINTS points (min/max per bin) and says so in the info line"""
        with mock.patch.object(figures, "MAX_POINTS", 10):
            fig, info, *_ = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full")
        ground = next(t for t in fig["data"] if t["name"] == "-0.10 m")
        self.assertLessEqual(len([y for y in ground["y"] if y is not None]), 10)
        self.assertIn("thinned", text_of(info))

    def test_api_timeout(self):
        """a 504 of the API (statement timeout): the timeout text in an alert, one request, no view"""
        self.server.force = fake_api.QUERY_TIMEOUT
        fig, info, message, view = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05")
        self.assertIn(texts.API_TIMEOUT, text_of(message))
        self.assertEqual(len(self.api_requests("measurements/daily")), 1)
        self.assertIsNone(view)


# ---- zoom -------------------------------------------------------------------------------------

class ZoomTest(PageTestCase):
    def full(self):
        fig, info, message, view = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full")
        self.server.requests.clear()
        return fig, view

    def test_zoom_window(self):
        """zoom in Full: exactly 1 /raw request with the window's bounds, the traces patched by key"""
        fig, view = self.full()
        r = self.zoom({"xaxis3.range[0]": "2026-10-02 00:00:00", "xaxis3.range[1]": "2026-10-03 12:30:00.5"}, view)
        daily, raw = self.measurement_requests()
        self.assertEqual(daily, [])
        self.assertEqual([(p["start"], p["end"]) for p in raw], [("2026-10-02T00:00:00Z", "2026-10-03T12:30:00Z")])
        patch = r["data-graph"]["figure"]
        ops = patch["operations"]
        xs = {op["location"][1] for op in ops if op["location"][-1] == "x"}
        self.assertEqual(xs, set(range(len(view["keys"]))))
        ground_x = next(op["params"]["value"] for op in ops if op["location"] == ["data", 0, "x"])
        self.assertEqual((ground_x[0], ground_x[-1]), ("2026-10-02T00:00:00", "2026-10-03T12:00:00"))
        self.assertIn("Zoomed", text_of(r["info"]["children"]))
        self.assertIsNone(r["message"]["children"])
        titles = {op["location"][2]: op["params"]["value"] for op in ops if op["location"][:2] == ["layout", "annotations"]}
        self.assertEqual(titles, {i: texts.ROW_FULL.format(title=t) for i, t in enumerate(CAMPBELL_ROWS)})
        window = r["view"]["data"]["operations"]
        self.assertEqual(window[0]["params"]["value"], ["2026-10-02T00:00:00+00:00", "2026-10-03T12:30:00+00:00"])

    def test_zoom_thinned_titles(self):
        """zoom in Full: the row titles follow the window (thinned over the whole range, all measurements when zoomed in)"""
        with mock.patch.object(figures, "MAX_POINTS", 20):
            fig, view = self.full()
            self.assertIn("thinned", full_titles(fig)[0])
            r = self.zoom({"xaxis.range[0]": "2026-10-02 00:00:00", "xaxis.range[1]": "2026-10-02 12:00:00"}, view)
        ops = r["data-graph"]["figure"]["operations"]
        titles = {op["location"][2]: op["params"]["value"] for op in ops if op["location"][:2] == ["layout", "annotations"]}
        self.assertEqual(titles[0], texts.ROW_FULL.format(title=texts.ROW_GROUND))

    def test_zoom_reset(self):
        """autorange (reset) in Full: 1 /raw request for the whole loaded range"""
        _, view = self.full()
        r = self.zoom({"xaxis.autorange": True, "xaxis2.autorange": True}, view)
        self.assertEqual([(p["start"], p["end"]) for p in self.api_requests("measurements/raw")],
                         [("2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertIsNone(r["view"]["data"]["operations"][0]["params"]["value"])     # no window

    def test_zoom_daily_no_request(self):
        """zoom in Daily: no request, the figure unchanged, only the window recorded in the view; a relayout without an x range (autosize, y only): nothing"""
        *_, view = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05")
        self.server.requests.clear()
        r = self.zoom({"xaxis.range[0]": "2026-10-02", "xaxis.range[1]": "2026-10-03"}, view)
        self.assertEqual(set(r), {"view"})
        op = r["view"]["data"]["operations"][0]
        self.assertEqual((op["location"], op["params"]["value"]),
                         (["window"], ["2026-10-02T00:00:00+00:00", "2026-10-03T00:00:00+00:00"]))
        _, full_view = self.full()
        self.assertEqual(self.zoom({"autosize": True}, full_view), {})
        self.assertEqual(self.zoom({"yaxis2.range[0]": 1, "yaxis2.range[1]": 2}, full_view), {})
        self.assertEqual(self.server.requests, [])

    def test_zoom_error(self):
        """a 504 on a zoom: the timeout text in an alert, the figure left as it is"""
        _, view = self.full()
        self.server.force = fake_api.QUERY_TIMEOUT
        r = self.zoom({"xaxis.range[0]": "2026-10-02", "xaxis.range[1]": "2026-10-03"}, view)
        self.assertNotIn("data-graph", r)
        self.assertNotIn("view", r)
        self.assertIn(texts.API_TIMEOUT, text_of(r["message"]["children"]))
        self.assertEqual(len(self.api_requests("measurements/raw")), 1)

    def test_zoom_target(self):
        """zoom_target reads the x range of any matched row, autorange, and ignores y-only changes"""
        utc = dt.timezone.utc
        self.assertEqual(callbacks.zoom_target({"xaxis5.range[0]": "2026-10-02 06:00", "xaxis5.range[1]": "2026-10-02T03:00Z"}),
                         (dt.datetime(2026, 10, 2, 3, tzinfo=utc), dt.datetime(2026, 10, 2, 6, tzinfo=utc)))
        self.assertEqual(callbacks.zoom_target({"xaxis2.range": ["2026-10-02", "2026-10-03"]})[1],
                         dt.datetime(2026, 10, 3, tzinfo=utc))
        self.assertEqual(callbacks.zoom_target({"xaxis3.autorange": True}), "overview")
        self.assertIsNone(callbacks.zoom_target({"yaxis.range[0]": 0, "yaxis.range[1]": 1}))
        self.assertIsNone(callbacks.zoom_target(None))


class PanTest(PageTestCase):
    """Full over a period longer than a year loads its last year; a pan or zoom then loads the
    visible window anywhere in the selected period (T16, item 8)."""

    def full_long(self):
        *_, view = self.load(CAMPBELL_M6, "2024-01-01", "2026-10-05", "full")
        self.server.requests.clear()
        return view

    def raw_bounds(self):
        return [(p["start"], p["end"]) for p in self.api_requests("measurements/raw")]

    def view_ops(self, r):
        return {op["location"][0]: op["params"]["value"] for op in r["view"]["data"]["operations"]}

    def test_pan_before_loaded_year(self):
        """pan to a window before the first loaded year: exactly 1 /raw with that window's bounds, the traces filled, the view's range updated"""
        view = self.full_long()
        self.assertEqual(view["range"], ["2025-10-06T00:00:00+00:00", "2026-10-05T23:59:59+00:00"])
        r = self.zoom({"xaxis.range[0]": "2024-03-01 00:00:00", "xaxis.range[1]": "2024-03-20 00:00:00"}, view)
        self.assertEqual(self.raw_bounds(), [("2024-03-01T00:00:00Z", "2024-03-20T00:00:00Z")])
        self.assertIn("data-graph", r)
        ops = self.view_ops(r)
        self.assertEqual(ops["range"], ["2024-03-01T00:00:00+00:00", "2024-03-20T00:00:00+00:00"])
        self.assertEqual(ops["window"], ops["range"])

    def test_pan_fills_traces(self):
        """a pan to the days with data (the stub has them in October 2026 only) fills the traces"""
        *_, view = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full")
        view = dict(view, range=["2026-10-04T00:00:00+00:00", "2026-10-05T23:59:59+00:00"])     # as after a zoom
        self.server.requests.clear()
        r = self.zoom({"xaxis.range[0]": "2026-10-01 00:00:00", "xaxis.range[1]": "2026-10-02 00:00:00"}, view)
        self.assertEqual(self.raw_bounds(), [("2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")])
        x = next(op["params"]["value"] for op in r["data-graph"]["figure"]["operations"] if op["location"] == ["data", 0, "x"])
        self.assertEqual((x[0], x[-1]), ("2026-10-01T00:00:00", "2026-10-02T00:00:00"))

    def test_window_longer_than_a_year(self):
        """a window wider than a year loads the year up to the window's end, and the info line says so"""
        view = self.full_long()
        r = self.zoom({"xaxis.range[0]": "2024-02-01 00:00:00", "xaxis.range[1]": "2026-06-30 12:00:00"}, view)
        self.assertEqual(self.raw_bounds(), [("2025-07-01T00:00:00Z", "2026-06-30T12:00:00Z")])
        self.assertIn(texts.INFO_ZOOM_LIMIT, text_of(r["info"]["children"]))

    def test_window_partly_outside(self):
        """a window partly outside the selected period is cut to it; entirely outside: no request"""
        view = self.full_long()
        self.zoom({"xaxis.range[0]": "2023-12-01 00:00:00", "xaxis.range[1]": "2024-01-10 00:00:00"}, view)
        self.assertEqual(self.raw_bounds(), [("2024-01-01T00:00:00Z", "2024-01-10T00:00:00Z")])
        self.server.requests.clear()
        self.assertEqual(self.zoom({"xaxis.range[0]": "2026-11-01 00:00:00", "xaxis.range[1]": "2026-11-20 00:00:00"}, view), {})
        self.assertEqual(self.raw_bounds(), [])

    def test_reset_after_pan(self):
        """autorange after a pan goes back to the first Full load (the last year of the selected period)"""
        view = self.full_long()
        r = self.zoom({"xaxis.range[0]": "2024-03-01 00:00:00", "xaxis.range[1]": "2024-03-20 00:00:00"}, view)
        view = dict(view, **self.view_ops(r))
        self.server.requests.clear()
        r = self.zoom({"xaxis.autorange": True}, view)
        self.assertEqual(self.raw_bounds(), [("2025-10-06T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertIn(texts.INFO_FULL_LIMIT, text_of(r["info"]["children"]))
        self.assertEqual(self.view_ops(r)["range"], ["2025-10-06T00:00:00+00:00", "2026-10-05T23:59:59+00:00"])

# ---- the heading: what is drawn (T16) ---------------------------------------------------------

class HeadingTest(PageTestCase):
    def setUp(self):
        super().setUp()
        self.catalogue = self.masts()["masts"]["data"]
        self.server.requests.clear()

    def heading(self):
        return text_of(self.loaded["data-heading"]["children"])

    def test_single_logger(self):
        """the heading names the drawn data: mast, station, logger, Daily/Full and the period shown"""
        self.load_all(CAMPBELL_M6, "2026-10-01", "2026-10-05", masts=self.catalogue)
        self.assertEqual(self.heading(), "Mast 6 · SIS2024-01 · CR300_M6 (12096) · Daily means · 2026-10-01 to 2026-10-05")
        self.load_all(CAMPBELL_M6, "2024-01-01", "2026-10-05", "full", masts=self.catalogue)
        self.assertEqual(self.heading(), "Mast 6 · SIS2024-01 · CR300_M6 (12096) · Full resolution · 2025-10-06 to 2026-10-05")

    def test_all_loggers(self):
        """All loggers: the mast, its stations and the loggers drawn"""
        self.load_all("all:Mast 6", "2026-09-01", "2026-10-05", masts=self.catalogue)
        self.assertEqual(self.heading(), "Mast 6 · SIS2024-01 · All loggers: Tekbox_TBSL1_3 (feedbeefcafe0002); "
                                         "CR300_M6 (12096) · Daily means · 2026-09-01 to 2026-10-05")
        self.load_all("all:Mast 6", "2026-10-01", "2026-10-05", masts=self.catalogue)
        self.assertIn("All loggers: CR300_M6 (12096) · ", self.heading())

    def test_error_clears(self):
        """a failed load: an empty figure (never the previous data), the heading names the selection and the problem; no data: the same with no data"""
        self.load_all(CAMPBELL_M6, "2026-10-01", "2026-10-05", masts=self.catalogue)
        self.server.force = fake_api.QUERY_TIMEOUT
        fig, info, message, view, store = self.load_all(CAMPBELL_M13, "2026-10-01", "2026-10-05", masts=self.catalogue)
        self.assertEqual(fig["data"], [])
        self.assertIsNone(view)
        self.assertIsNone(store)
        self.assertEqual(self.loaded["profile-body"]["style"], {"display": "none"})
        text = self.heading()
        self.assertTrue(text.startswith("Mast 13 · SIS2024-05 · CR300_M13 (12216) · Daily means · 2026-10-01 to 2026-10-05"), text)
        self.assertIn(texts.HEADING_FAILED.format(reason=texts.API_TIMEOUT), text)
        self.server.force = None
        fig, *_ = self.load_all(CAMPBELL_M6, "2025-01-01", "2025-01-10", masts=self.catalogue)
        self.assertEqual(fig["data"], [])
        self.assertTrue(self.heading().endswith(" · " + texts.HEADING_NO_DATA), self.heading())

    def test_zoom_heading(self):
        """a zoom in Full rewrites the heading's period to the window drawn; the profile store names its mast and logger"""
        *_, view, store = self.load_all(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full", masts=self.catalogue)
        self.assertEqual(store["source"], "Mast 6 · SIS2024-01 · CR300_M6 (12096)")
        r = self.zoom({"xaxis.range[0]": "2026-10-02 00:00:00", "xaxis.range[1]": "2026-10-03 12:00:00"}, view)
        self.assertEqual(text_of(r["data-heading"]["children"]),
                         "Mast 6 · SIS2024-01 · CR300_M6 (12096) · Full resolution · 2026-10-02 00:00 to 2026-10-03 12:00")


# ---- the Daily/Full switch keeps the zoom (T16) ------------------------------------------------

class SwitchTest(PageTestCase):
    def switch(self, logger, start, end, resolution, view, masts=None):
        """load_data as the Daily/Full switch triggers it, with the view store before."""
        self.server.requests.clear()
        return self.load_all(logger, start, end, resolution, masts=masts, view=view,
                             changed="resolution.value")

    def test_daily_to_full_zoomed(self):
        """Daily -> Full while zoomed: 1 /raw for the visible window only and 1 /daily for the profile (the days up to the window's end); the new figure keeps the window as its x range"""
        *_, view = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05")
        view = dict(view, window=["2026-10-02T06:00:00+00:00", "2026-10-03T18:00:00+00:00"])
        fig, info, message, new_view, store = self.switch(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full", view)
        daily, raw = self.measurement_requests()
        self.assertEqual([(p["start"], p["end"]) for p in raw], [("2026-10-02T06:00:00Z", "2026-10-03T18:00:00Z")])
        self.assertEqual([(p["start"], p["end"]) for p in daily], [("2026-10-01T00:00:00Z", "2026-10-03T23:59:59Z")])
        self.assertIsNone(message)
        self.assertEqual(fig["layout"]["xaxis"]["range"], ["2026-10-02 06:00:00", "2026-10-03 18:00:00"])
        self.assertEqual(new_view["mode"], "full")
        self.assertEqual(new_view["window"], ["2026-10-02T06:00:00+00:00", "2026-10-03T18:00:00+00:00"])
        self.assertEqual(new_view["range"], ["2026-10-01T00:00:00+00:00", "2026-10-03T23:59:59+00:00"])
        self.assertIn("Zoomed", text_of(info))
        self.assertEqual(store["n"], 3)
        ground = next(t for t in fig["data"] if t["name"] == "-0.10 m")
        self.assertGreaterEqual(ground["x"][0], "2026-10-02T06:00:00")
        self.assertLessEqual(ground["x"][-1], "2026-10-03T18:00:00")

    def test_full_to_daily_zoomed(self):
        """Full -> Daily while zoomed: 1 /daily for the whole period, as unzoomed, and the x range set to the window"""
        *_, view = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full")
        view = dict(view, window=["2026-10-02T00:00:00+00:00", "2026-10-03T00:00:00+00:00"])
        fig, info, message, new_view, _ = self.switch(CAMPBELL_M6, "2026-10-01", "2026-10-05", "daily", view)
        daily, raw = self.measurement_requests()
        self.assertEqual(raw, [])
        self.assertEqual([(p["start"], p["end"]) for p in daily], [("2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertEqual(fig["layout"]["xaxis"]["range"], ["2026-10-02 00:00:00", "2026-10-03 00:00:00"])
        self.assertEqual(new_view["window"], view["window"])
        self.assertEqual(new_view["range"], ["2026-10-01T00:00:00+00:00", "2026-10-05T23:59:59+00:00"])

    def test_unzoomed_unchanged(self):
        """the switch without a zoom, or another change (logger, period) with a zoom: the whole period as before, no x range set"""
        *_, view = self.load(CAMPBELL_M6, "2026-10-01", "2026-10-05")
        fig, *_ = self.switch(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full", view)
        self.assertEqual([(p["start"], p["end"]) for p in self.api_requests("measurements/raw")],
                         [("2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        zoomed = dict(view, window=["2026-10-02T00:00:00+00:00", "2026-10-03T00:00:00+00:00"])
        self.server.requests.clear()
        fig, info, message, new_view, _ = self.load_all(CAMPBELL_M6, "2026-10-01", "2026-10-04", "full", view=zoomed,
                                                        changed="period.end_date")
        self.assertEqual([(p["start"], p["end"]) for p in self.api_requests("measurements/raw")],
                         [("2026-10-01T00:00:00Z", "2026-10-04T23:59:59Z")])
        self.assertIsNone(new_view["window"])
        fig, *_ = self.switch(CAMPBELL_M6, "2026-10-01", "2026-10-05", "daily", dict(zoomed, logger=TEKBOX_M6))
        self.assertNotIn("range", fig["layout"]["xaxis"])        # a window of another logger is not kept

    def test_zoom_longer_than_a_year(self):
        """Daily -> Full with a window longer than a year: its last year only (1 /raw), and the info line says so"""
        *_, view = self.load(CAMPBELL_M6, "2024-01-01", "2026-10-05")
        view = dict(view, window=["2024-06-01T00:00:00+00:00", "2026-10-02T12:00:00+00:00"])
        fig, info, message, new_view, _ = self.switch(CAMPBELL_M6, "2024-01-01", "2026-10-05", "full", view)
        daily, raw = self.measurement_requests()
        self.assertEqual([(p["start"], p["end"]) for p in raw], [("2025-10-03T00:00:00Z", "2026-10-02T12:00:00Z")])
        self.assertEqual([(p["start"], p["end"]) for p in daily], [("2025-10-03T00:00:00Z", "2026-10-02T23:59:59Z")])
        self.assertIn(texts.INFO_ZOOM_LIMIT, text_of(info))
        self.assertEqual(fig["layout"]["xaxis"]["range"], ["2025-10-03 00:00:00", "2026-10-02 12:00:00"])

    def test_all_loggers_daily_to_full_zoomed(self):
        """All loggers, Daily -> Full zoomed into one logger's days: 1 /raw (that logger, the window) + the profile's /daily per logger with data in its days"""
        masts = self.masts()["masts"]["data"]
        *_, view = self.load_all("all:Mast 6", "2026-09-01", "2026-10-05", masts=masts)
        view = dict(view, window=["2026-10-02T00:00:00+00:00", "2026-10-03T00:00:00+00:00"])
        fig, info, message, new_view, store = self.switch("all:Mast 6", "2026-09-01", "2026-10-05", "full", view, masts)
        daily, raw = self.measurement_requests()
        self.assertEqual([(p["start"], p["end"]) for p in raw], [("2026-10-02T00:00:00Z", "2026-10-03T00:00:00Z")])
        self.assertEqual(raw[0].get("serial"), "12096")
        self.assertEqual(len(daily), 2)         # the profile, as an unzoomed Full load: both loggers in its days
        self.assertEqual(fig["layout"]["xaxis"]["range"], ["2026-10-02 00:00:00", "2026-10-03 00:00:00"])
        self.assertEqual(new_view["window"], view["window"])


if __name__ == "__main__":
    unittest.main()
