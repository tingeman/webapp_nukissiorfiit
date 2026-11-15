"""HTTP checks of a running nuki container, run inside it with the image's python by
tests/run.sh (docker exec <app> python /tests/smoke_check.py <check>), against the app on
127.0.0.1:8050. Prints "OK <details>" and exits 0, or prints the problem and exits 1.

  index      GET /app/nuki/ returns 200 and the Dash page with the /app/nuki/ prefix
  callback   POST /app/nuki/_dash-update-component for the callbacks of app.py in the order
             the page runs them, each fed with the previous answers: load_catalogue (masts of
             the stub API: Mast 6, Mast 9, Mast 13), update_loggers (Mast 6: 2 loggers,
             default the Campbell; Mast 13: only CR300_M13), update_date_range (the last 30
             days of the logger's data, within its first..last dates) and
             get_data_from_measurement for a Tekbox (deveui) and a Campbell (station + serial)
             logger: six figures built from the stub data (tests/fixtures/make_fixtures.py
             describes the values) and the ground temperature table; and without a logger,
             six "Select a mast and logger" figures
"""
import base64
import json
import re
import sys
import urllib.error
import urllib.request

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


def call(outputs, inputs, state=(), changed=None):
    """POST one callback: outputs [(id, prop)], inputs/state [(id, prop, value)]. Returns
    {id: {prop: value}} of the outputs the callback set."""
    payload = {
        "output": ".." + "...".join(f"{i}.{p}" for i, p in outputs) + "..",
        "outputs": [{"id": i, "property": p} for i, p in outputs],
        "inputs": [{"id": i, "property": p, "value": v} for i, p, v in inputs],
        "changedPropIds": [changed or f"{inputs[0][0]}.{inputs[0][1]}"],
        "state": [{"id": i, "property": p, "value": v} for i, p, v in state],
    }
    req = urllib.request.Request(
        BASE + "_dash-update-component", data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    name = payload["output"]
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            status, body = r.status, r.read()
    except urllib.error.HTTPError as e:
        fail(f"POST {name}: HTTP {e.code}: {e.read()[:300]!r}")
    if status != 200:
        fail(f"POST {name}: HTTP {status}")
    return json.loads(body)["response"]


def check_index():
    try:
        with urllib.request.urlopen(BASE, timeout=20) as r:
            status, body = r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        fail(f"GET {BASE}: HTTP {e.code}")
    if status != 200:
        fail(f"GET {BASE}: HTTP {status}")
    m = re.search(r'<script id="_dash-config" type="application/json">(.*?)</script>', body, re.S)
    if not m:
        fail(f"GET {BASE}: no _dash-config in the page: {body[:300]}")
    prefix = json.loads(m.group(1)).get("requests_pathname_prefix")
    if prefix != "/app/nuki/":
        fail(f"GET {BASE}: requests_pathname_prefix is {prefix!r}, expected '/app/nuki/'")
    print(f"OK 200, {len(body)} bytes, prefix {prefix}")


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


if __name__ == "__main__":
    {"index": check_index, "callback": check_callback}.get(
        sys.argv[1] if len(sys.argv) > 1 else "", lambda: fail(__doc__))()
