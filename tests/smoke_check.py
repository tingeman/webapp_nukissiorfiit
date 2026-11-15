"""HTTP checks of a running nuki container, run inside it with the image's python by
tests/run.sh (docker exec <app> python /tests/smoke_check.py <check>), against the app on
127.0.0.1:8050. Prints "OK <details>" and exits 0, or prints the problem and exits 1.

  index      GET /app/nuki/ returns 200 and the Dash page with the /app/nuki/ prefix
  v2index    GET /app/nuki/v2/ returns 200 and the phase 2 Dash page with the /app/nuki/v2/
             prefix (a second Dash app in the same gunicorn process)
  callback   POST /app/nuki/_dash-update-component for the callbacks of app.py in the order
             the page runs them, each fed with the previous answers: load_catalogue (masts of
             the stub API: Mast 6, Mast 9, Mast 13), update_loggers (Mast 6: 2 loggers,
             default the Campbell; Mast 13: only CR300_M13), update_date_range (the last 30
             days of the logger's data, within its first..last dates) and
             get_data_from_measurement for a Tekbox (deveui) and a Campbell (station + serial)
             logger: six figures built from the stub data (tests/fixtures/make_fixtures.py
             describes the values) and the ground temperature table; and without a logger,
             six "Select a mast and logger" figures
  v2callback POST /app/nuki/v2/_dash-update-component for the phase 2 page's callbacks in
             the order the page runs them: load_masts (Mast 6, Mast 9, Mast 13), fill_loggers
             (Mast 6: default the Campbell, the logger with the latest data), set_period (its
             first..last day), load_data Daily (one figure: ground temperature, weather,
             pressure in kPa, the two angles, sensor temperature, battery; one line of
             daily means per series, no bands, date tick labels on every row; the heading
             naming mast, station, logger, values and period) and Full (the same rows, one
             line per series), each with the profile store (the 3 ground temperature
             depths, the annual envelope and the figure template) and the profile slider
             (on the last day), zoom_data on a window of the Full figure (its traces
             patched, the window recorded in the view), the clientside profile callback
             (registered in _dash-dependencies, assets/profile.js served), and "All
             loggers" of Mast 6 in Daily (the dropdown's last option, the period over both
             loggers, one line per depth over both, a dashed line at the logger change)
  v2download POST /app/nuki/v2/_dash-update-component for the download card: the checklists
             of Mast 6 (both loggers and "All" ticked), the year dropdowns (2026..2026) and the
             button (All, 2026): dcc.send_bytes data of nuki_Mast_6_2026-2026.xlsx that opens
             with openpyxl, with the Info sheet and a full and a daily sheet per logger (33
             and 5 data rows)
"""
import base64
import json
import re
import sys
import urllib.error
import urllib.request

import io

import numpy as np

BASE = "http://127.0.0.1:8050/app/nuki/"

TEKBOX_M6 = "deveui:feedbeefcafe0002"      # device 10, 2026-09-01..05
CAMPBELL_M6 = "serial:SIS2024-01:12096"    # device 11, 2026-10-01..05
CAMPBELL_M13 = "serial:SIS2024-05:12216"   # device 41; the Mast 13 Tekbox has no data

# (title suffix, expected date range, {title: (trace names, first 3 y values of the first
# trace)}) per logger. Values from make_fixtures.py: k = 3-hour steps since the start.
EXPECTED = {
    TEKBOX_M6: ("Mast 6 (Tekbox_TBSL1_3 feedbeefcafe0002)",
                ("2026-09-01", "2026-09-05", "2026-09-01", "2026-09-05"), {
        "Battery Level": (["Battery level"], [3.7, 3.699, 3.698]),
        "Ground Temperature": (["-0.10 m", "-0.25 m", "-0.50 m"], [-0.1, -0.102, -0.104]),
        "Air Temperature": (["Air Temperature"], [2.0, 1.95, 1.9]),
        "Relative Humidity": (["Relative Humidity"], [80.0, 80.1, 80.2]),
        "Barometric Pressure": (["Barometric Pressure"], [101.3, 101.31, 101.32]),
        "Inclination": (["PitchAngle", "RollAngle"], [-0.8, -0.801, -0.802]),
    }),
    # the inclinometer is 6-hourly: its rows are 00:00, 06:00, 12:00 (k = 0, 2, 4)
    CAMPBELL_M6: ("Mast 6 (CR300_M6 12096)",
                  ("2026-10-01", "2026-10-05", "2026-10-01", "2026-10-05"), {
        "Battery Level": (["Battery level"], [13.0, 12.998, 12.996]),
        "Ground Temperature": (["-0.10 m", "-0.25 m", "-0.50 m"], [-1.1, -1.102, -1.104]),
        "Air Temperature": (["Air Temperature"], [3.0, 2.95, 2.9]),
        "Relative Humidity": (["Relative Humidity"], [80.0, 80.1, 80.2]),
        "Barometric Pressure": (["Barometric Pressure"], [101.3, 101.31, 101.32]),
        "Inclination": (["XAngle", "YAngle"], [0.45, 0.452, 0.454]),
    }),
}
Y_LABELS = {"Battery Level": "Voltage (V)", "Ground Temperature": "Temperature (°C)",
            "Air Temperature": "Temperature (°C)", "Relative Humidity": "Relative Humidity (%)",
            "Barometric Pressure": "Pressure (kPa)", "Inclination": "Inclination (deg)"}
TITLES = list(Y_LABELS)       # page order
TABLE_COLUMNS = ["GroundTemp01", "GroundTemp02", "GroundTemp03"]
TABLE_ROWS = 33               # 3-hourly, 5 days, both ends included


def fail(msg):
    print(msg)
    sys.exit(1)


def values(v):
    """Plotly 6 sends numeric arrays as {"dtype", "bdata"} (base64); plain lists otherwise."""
    if isinstance(v, dict) and "bdata" in v:
        return np.frombuffer(base64.b64decode(v["bdata"]), dtype=v["dtype"]).tolist()
    return list(v)


def figures(node):
    """All dcc.Graph figures in a Dash component tree (JSON), in order."""
    if isinstance(node, list):
        for item in node:
            yield from figures(item)
    elif isinstance(node, dict):
        if node.get("type") == "Graph" and "figure" in node.get("props", {}):
            yield node["props"]["figure"]
        for value in node.get("props", {}).values():
            yield from figures(value)


def layout_text(fig, key):
    t = fig.get("layout", {}).get(key, {})
    if key == "yaxis":
        t = t.get("title", {})
    return t.get("text") if isinstance(t, dict) else t


def call(outputs, inputs, state=(), changed=None, base=BASE):
    """POST one callback: outputs [(id, prop)] (a prop may carry Dash's "@<hash>" of an
    allow_duplicate output), inputs/state [(id, prop, value)]. Returns {id: {prop: value}} of
    the outputs the callback set ({} when it changed nothing: HTTP 204)."""
    payload = {
        "output": ".." + "...".join(f"{i}.{p}" for i, p in outputs) + "..",
        "outputs": [{"id": i, "property": p} for i, p in outputs],
        "inputs": [{"id": i, "property": p, "value": v} for i, p, v in inputs],
        "changedPropIds": [changed or f"{inputs[0][0]}.{inputs[0][1]}"],
        "state": [{"id": i, "property": p, "value": v} for i, p, v in state],
    }
    req = urllib.request.Request(
        base + "_dash-update-component", data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    name = payload["output"]
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            status, body = r.status, r.read()
    except urllib.error.HTTPError as e:
        fail(f"POST {name}: HTTP {e.code}: {e.read()[:300]!r}")
    if status == 204:
        return {}
    if status != 200:
        fail(f"POST {name}: HTTP {status}")
    return json.loads(body)["response"]


def check_index(url=BASE, expected_prefix="/app/nuki/"):
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            status, body = r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        fail(f"GET {url}: HTTP {e.code}")
    if status != 200:
        fail(f"GET {url}: HTTP {status}")
    m = re.search(r'<script id="_dash-config" type="application/json">(.*?)</script>', body, re.S)
    if not m:
        fail(f"GET {url}: no _dash-config in the page: {body[:300]}")
    prefix = json.loads(m.group(1)).get("requests_pathname_prefix")
    if prefix != expected_prefix:
        fail(f"GET {url}: requests_pathname_prefix is {prefix!r}, expected {expected_prefix!r}")
    print(f"OK 200, {len(body)} bytes, prefix {prefix}")


def check_v2_index():
    check_index(BASE + "v2/", "/app/nuki/v2/")


def load_catalogue():
    r = call([("catalogue", "data"), ("mast", "options"), ("mast", "value")], [("mast", "id", "mast")])
    catalogue = r["catalogue"]["data"]
    if catalogue.get("error"):
        fail(f"catalogue: error {catalogue['error']!r}")
    names = [o["value"] for o in r["mast"]["options"]]
    if names != ["Mast 6", "Mast 9", "Mast 13"] or [o["label"] for o in r["mast"]["options"]] != names:
        fail(f"mast options: {r['mast']['options']}")
    if r["mast"]["value"] != "Mast 6":
        fail(f"mast value: {r['mast']['value']!r}")
    return catalogue


def loggers(catalogue, mast):
    r = call([("logger", "options"), ("logger", "value")], [("mast", "value", mast)],
             state=[("catalogue", "data", catalogue)])
    return [o["value"] for o in r["logger"]["options"]], r["logger"]["value"]


def date_range(catalogue, logger):
    props = ["start_date", "end_date", "min_date_allowed", "max_date_allowed", "initial_visible_month"]
    r = call([("date-range-picker", p) for p in props], [("logger", "value", logger)],
             state=[("catalogue", "data", catalogue)])
    return tuple(r["date-range-picker"][p] for p in props)


def data(catalogue, logger, start, end):
    r = call([("graph", "children"), ("table", "rowData"), ("table", "columnDefs")],
             [("logger", "value", logger), ("date-range-picker", "start_date", start),
              ("date-range-picker", "end_date", end), ("catalogue", "data", catalogue)])
    return list(figures(r["graph"]["children"])), r["table"]["rowData"], [c["field"] for c in r["table"]["columnDefs"]]


def check_data(catalogue, logger):
    where, dates, expected = EXPECTED[logger]
    got_dates = date_range(catalogue, logger)
    if got_dates[:4] != dates or got_dates[4] != dates[1]:
        fail(f"{logger}: date range (start, end, min, max, visible month) {got_dates}, expected {dates}")
    figs, rows, cols = data(catalogue, logger, dates[0], dates[1])
    titles = [layout_text(f, "title") for f in figs]
    if titles != [f"{t} - {where}" for t in TITLES]:
        fail(f"{logger}: figure titles {titles}")
    for title, fig in zip(TITLES, figs):
        names, ys = expected[title]
        traces = fig.get("data", [])
        got_names = [t.get("name") for t in traces]
        if got_names != names:
            fail(f"{logger}: {title}: trace names {got_names}, expected {names} (texts: {[t.get('text') for t in traces]})")
        got = [round(y, 6) for y in values(traces[0]["y"])[:3]]
        if got != ys:
            fail(f"{logger}: {title}: y values {got}, expected {ys}")
        if layout_text(fig, "yaxis") != Y_LABELS[title]:
            fail(f"{logger}: {title}: y label {layout_text(fig, 'yaxis')!r}, expected {Y_LABELS[title]!r}")
    if cols != TABLE_COLUMNS or len(rows) != TABLE_ROWS:
        fail(f"{logger}: table: columns {cols}, {len(rows)} rows, expected {TABLE_COLUMNS}, {TABLE_ROWS} rows")
    return len(figs)


def check_callback():
    catalogue = load_catalogue()
    options, value = loggers(catalogue, "Mast 6")
    if sorted(options) != sorted([TEKBOX_M6, CAMPBELL_M6]) or value != CAMPBELL_M6:
        fail(f"Mast 6 loggers: options {options}, value {value!r}")
    options, value = loggers(catalogue, "Mast 13")
    if options != [CAMPBELL_M13] or value != CAMPBELL_M13:
        fail(f"Mast 13 loggers: options {options}, value {value!r}")
    n = [check_data(catalogue, lg) for lg in (TEKBOX_M6, CAMPBELL_M6)]
    figs, rows, cols = data(catalogue, None, None, None)
    texts = [t.get("text") for f in figs for t in f.get("data", [])]
    if texts != ["Select a mast and logger"] * 6 or rows or cols:
        fail(f"no logger: texts {texts}, table {cols} {len(rows)} rows")
    print(f"OK 3 masts, Mast 6: 2 loggers, Mast 13: 1 logger; {n[0]} and {n[1]} figures for a Tekbox and a Campbell "
          f"logger from the stub data, table {TABLE_ROWS} rows; no logger: 6 messages")


# ---- phase 2 page ------------------------------------------------------------------------------

V2 = BASE + "v2/"
V2_ROWS = ["Ground temperature", "Air temperature", "Relative humidity", "Barometric pressure",
           "Inclination XAngle", "Inclination YAngle", "Sensor temperature", "Battery voltage"]


def v2_call(outputs, inputs, state=()):
    return call(outputs, inputs, state, base=V2)


def v2_duplicate_outputs(outputs):
    """The output spec of the v2 callback with these outputs (allow_duplicate ones carry
    "@<hash>"), from the page's _dash-dependencies."""
    with urllib.request.urlopen(V2 + "_dash-dependencies", timeout=20) as r:
        deps = json.loads(r.read())
    wanted = [f"{i}.{p}" for i, p in outputs]
    for d in deps:
        parts = d["output"].strip(".").split("...")
        if [re.sub(r"@[0-9a-f]+$", "", x) for x in parts] == wanted:
            return [tuple(x.rsplit(".", 1)) for x in parts]
    fail(f"v2: no callback with the outputs {wanted}")


V2_LOAD_OUTPUTS = ([("data-graph", "figure"), ("info", "children"), ("message", "children"), ("view", "data"),
                    ("profile", "data")] + [("profile-day", p) for p in ("min", "max", "marks", "value")]
                   + [("profile-body", "style"), ("profile-note", "children"), ("data-heading", "children")])


def text_of(node):
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return " ".join(text_of(n) for n in node)
    if isinstance(node, dict):
        return text_of(node.get("props", {}).get("children"))
    return ""


def v2_load_raw(logger, start, end, resolution, masts=None):
    r = v2_call(V2_LOAD_OUTPUTS,
                [("logger", "value", logger), ("period", "start_date", start),
                 ("period", "end_date", end), ("resolution", "value", resolution)],
                state=[("masts", "data", masts), ("view", "data", None)])
    if r["message"]["children"]:
        fail(f"v2 {logger} {resolution}: message {json.dumps(r['message']['children'])[:300]}")
    return r


def v2_load(logger, start, end, resolution, masts=None):
    r = v2_load_raw(logger, start, end, resolution, masts)
    fig, view = r["data-graph"]["figure"], r["view"]["data"]
    titles = [a.get("text") for a in fig.get("layout", {}).get("annotations", [])]
    suffix = ", daily mean" if resolution == "daily" else ", all measurements"
    if titles != [t + suffix for t in V2_ROWS]:
        fail(f"v2 {resolution}: row titles {titles}, expected {V2_ROWS} + {suffix!r}")
    axes = [k for k in fig["layout"] if re.fullmatch(r"xaxis\d*", k)]
    if len(axes) != len(V2_ROWS) or not all(fig["layout"][a].get("showticklabels") for a in axes):
        fail(f"v2 {resolution}: date tick labels not on every row")
    heading = text_of(r["data-heading"]["children"])
    mode = "Daily means" if resolution == "daily" else "Full resolution"
    if heading != f"Mast 6 · SIS2024-01 · CR300_M6 (12096) · {mode} · {start} to {end}":
        fail(f"v2 {resolution}: heading {heading!r}")
    slider = r["profile-day"]
    if slider["value"] != slider["max"] or slider["max"] - slider["min"] != 4 or r["profile-note"]["children"]:
        fail(f"v2 profile slider: {slider}, note {r['profile-note']['children']}")
    pressure = [t for t in fig["data"] if t.get("yaxis") == "y4"]
    p0 = values(pressure[0]["y"])[0] if pressure else None
    if p0 is None or abs(p0 - 101.3) > 0.2:
        fail(f"v2 {resolution}: first pressure value {p0}, expected about 101.3 (kPa)")
    store = r["profile"]["data"] or {}
    if store.get("depths") != [0.1, 0.25, 0.5] or store.get("n") != 5:
        fail(f"v2 {resolution}: profile store depths {store.get('depths')}, {store.get('n')} days")
    if [len(row) for row in store.get("amax", [])] != [5, 5, 5] or not store.get("draw", {}).get("layout"):
        fail(f"v2 {resolution}: profile store without the annual envelope or the figure template")
    return fig, view, store


def check_v2_profile():
    """The profile is drawn in the browser: the clientside callback is registered (inputs the
    slider, the envelope and the store) and its script is served."""
    with urllib.request.urlopen(V2 + "_dash-dependencies", timeout=20) as r:
        deps = json.loads(r.read())
    client = [d for d in deps if (d.get("clientside_function") or {}).get("namespace") == "nuki"]
    if len(client) != 1 or [i["id"] for i in client[0]["inputs"]] != ["profile-day", "profile-envelope", "profile"]:
        fail(f"v2 profile: clientside callback {json.dumps(client)[:300]}")
    with urllib.request.urlopen(V2 + "assets/profile.js", timeout=20) as r:
        js = r.read().decode()
    if "dash_clientside.nuki" not in js:
        fail("v2 profile: assets/profile.js without dash_clientside.nuki")
    return client[0]["clientside_function"]["function_name"]


def check_v2_all_loggers(masts):
    """All loggers of Mast 6, Daily: the period over both loggers, the ground temperature
    lines over both, broken at the change, one dashed line at the Campbell's first day."""
    value = "all:Mast 6"
    props = ["min_date_allowed", "max_date_allowed", "start_date", "end_date"]
    r = v2_call([("period", p) for p in props] + [("period", "initial_visible_month")],
                [("logger", "value", value)], state=[("masts", "data", masts)])
    period = [r["period"][p] for p in props]
    if period != ["2026-09-01", "2026-10-05", "2026-09-01", "2026-10-05"]:
        fail(f"v2 All loggers period: {period}")
    r = v2_load_raw(value, period[2], period[3], "daily", masts)
    fig, view = r["data-graph"]["figure"], r["view"]["data"] or {}
    titles = [a.get("text") for a in fig.get("layout", {}).get("annotations", [])]
    ground = [t for t in fig.get("data", []) if t.get("yaxis") in (None, "y") and "customdata" in t]
    spans = {(t["x"][0], t["x"][-1]) for t in ground}
    shapes = [(s.get("x0"), s.get("line", {}).get("dash")) for s in fig.get("layout", {}).get("shapes", [])]
    if len(ground) != 3 or spans != {("2026-09-01", "2026-10-05")} or not all(None in values(t["y"]) for t in ground):
        fail(f"v2 All loggers: ground traces {[t.get('name') for t in ground]}, spans {spans}")
    if shapes != [("2026-10-01 00:00:00", "dash")] or "CR300_M6 (12096)" not in titles:
        fail(f"v2 All loggers: change markers {shapes}, labels {titles}")
    if [lg.get("value") for lg in view.get("loggers", [])] != [TEKBOX_M6, CAMPBELL_M6]:
        fail(f"v2 All loggers: view {json.dumps(view)[:300]}")
    return len([t for t in titles if t != "CR300_M6 (12096)"])


def check_v2_callback():
    r = v2_call([("masts", "data"), ("mast", "options"), ("mast", "value"), ("masts-message", "children")],
                [("url", "pathname", "/app/nuki/v2/")])
    masts = r["masts"]["data"]
    names = [o["value"] for o in r["mast"]["options"]]
    if names != ["Mast 6", "Mast 9", "Mast 13"] or r["mast"]["value"] != "Mast 6":
        fail(f"v2 masts: options {names}, value {r['mast']['value']!r}, message {r['masts-message']['children']}")
    r = v2_call([("logger", "options"), ("logger", "value")], [("mast", "value", "Mast 6")],
                state=[("masts", "data", masts)])
    if r["logger"]["value"] != CAMPBELL_M6 or len(r["logger"]["options"]) != 3             or r["logger"]["options"][-1] != {"label": "All loggers", "value": "all:Mast 6"}:
        fail(f"v2 Mast 6 loggers: {r['logger']}")
    props = ["min_date_allowed", "max_date_allowed", "start_date", "end_date"]
    r = v2_call([("period", p) for p in props] + [("period", "initial_visible_month")],
                [("logger", "value", CAMPBELL_M6)], state=[("masts", "data", masts)])
    period = [r["period"][p] for p in props]
    if period != ["2026-10-01", "2026-10-05", "2026-10-01", "2026-10-05"]:
        fail(f"v2 period: {period}")
    fig, view, store = v2_load(CAMPBELL_M6, "2026-10-01", "2026-10-05", "daily", masts)
    profile_fn = check_v2_profile()
    bands = sum(1 for t in fig["data"] if t.get("fill"))
    if view.get("mode") != "daily" or bands or len(fig["data"]) != 10:
        fail(f"v2 daily: view {view.get('mode')}, {len(fig['data'])} traces, {bands} bands (expected 10, 0)")
    fig, view, _ = v2_load(CAMPBELL_M6, "2026-10-01", "2026-10-05", "full", masts)
    if view.get("mode") != "full" or [t.get("meta") for t in fig["data"]] != view.get("keys"):
        fail(f"v2 full: view {view}")
    outputs = v2_duplicate_outputs([("data-graph", "figure"), ("info", "children"), ("message", "children"),
                                    ("view", "data"), ("data-heading", "children")])
    r = call(outputs, [("data-graph", "relayoutData",
                        {"xaxis.range[0]": "2026-10-02 00:00:00", "xaxis.range[1]": "2026-10-03 00:00:00"})],
             state=[("view", "data", view)], base=V2)
    ops = (r.get("data-graph", {}).get("figure") or {}).get("operations") or []
    if len([op for op in ops if op.get("location", [None])[-1] == "x"]) != len(view["keys"]) \
            or "view" not in r:
        fail(f"v2 zoom: {json.dumps(r)[:300]}")
    n_all = check_v2_all_loggers(masts)
    print(f"OK v2: 3 masts, Mast 6 default {CAMPBELL_M6}, period {period[2]}..{period[3]}, "
          f"{len(V2_ROWS)} rows Daily (no bands) and Full ({len(view['keys'])} lines), zoom patched, "
          f"profile store and slider, clientside nuki.{profile_fn}; All loggers of Mast 6: {n_all} rows, "
          f"one change marker")


V2_SHEETS = ["Info", "Tekbox_TBSL1_3 cafe0002 full", "Tekbox_TBSL1_3 cafe0002 daily",
             "CR300_M6 (12096) full", "CR300_M6 (12096) daily"]


def check_v2_download():
    import openpyxl     # in the image (app/requirements.in)

    r = v2_call([("masts", "data"), ("mast", "options"), ("mast", "value"), ("masts-message", "children")],
                [("url", "pathname", "/app/nuki/v2/")])
    masts = r["masts"]["data"]
    r = v2_call([("download-loggers", "options"), ("download-loggers", "value"), ("download-all", "value")],
                [("mast", "value", "Mast 6"), ("download-all", "value", []), ("download-loggers", "value", [])],
                state=[("masts", "data", masts)])
    ticked = r["download-loggers"]["value"]
    if ticked != [TEKBOX_M6, CAMPBELL_M6] or r["download-all"]["value"] != ["all"]:
        fail(f"v2 download checklists: {r}")
    r = v2_call([("download-from", "options"), ("download-from", "value"), ("download-to", "options"),
                 ("download-to", "value")], [("download-loggers", "value", ticked)],
                state=[("mast", "value", "Mast 6"), ("masts", "data", masts)])
    years = (r["download-from"]["value"], r["download-to"]["value"])
    if years != (2026, 2026):
        fail(f"v2 download years: {r}")
    r = v2_call([("download-file", "data"), ("download-message", "children")], [("download-button", "n_clicks", 1)],
                state=[("mast", "value", "Mast 6"), ("download-loggers", "value", ticked),
                       ("download-from", "value", years[0]), ("download-to", "value", years[1]),
                       ("masts", "data", masts)])
    data = r.get("download-file", {}).get("data") or {}
    if data.get("filename") != "nuki_Mast_6_2026-2026.xlsx" or not data.get("base64"):
        fail(f"v2 download: {json.dumps(r)[:300]}")
    content = base64.b64decode(data["content"])
    try:
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
    except Exception as e:      # noqa: BLE001 - any problem opening it fails the check
        fail(f"v2 download: the xlsx does not open: {type(e).__name__}: {e}")
    if wb.sheetnames != V2_SHEETS:
        fail(f"v2 download: sheets {wb.sheetnames}")
    n = [sum(1 for _ in wb[name].iter_rows()) - 1 for name in V2_SHEETS[1:]]
    if n != [33, 5, 33, 5]:
        fail(f"v2 download: data rows {n}, expected [33, 5, 33, 5]")
    print(f"OK v2 download: Mast 6, All, 2026: {data['filename']}, {len(content)} bytes, sheets {V2_SHEETS}, "
          f"data rows {n}")


if __name__ == "__main__":
    {"index": check_index, "v2index": check_v2_index, "callback": check_callback,
     "v2callback": check_v2_callback, "v2download": check_v2_download}.get(
        sys.argv[1] if len(sys.argv) > 1 else "", lambda: fail(__doc__))()
