"""HTTP checks of a running nuki container, run inside it with the image's python by
tests/run.sh (docker exec <app> python /tests/smoke_check.py <check>), against the app on
127.0.0.1:8050. Prints "OK <details>" and exits 0, or prints the problem and exits 1.

  index      GET /app/nuki/ returns 200 and the Dash page with the /app/nuki/ prefix
  callback   POST /app/nuki/_dash-update-component for the main callback of app.py
             (inputs: mast "Mast 6", date range 2026-10-01..2026-10-01, the day the stub
             API has data for) returns six figures built from the stub data
             (tests/fixtures/measurements_v1.json) and the ground temperature table
"""
import base64
import json
import re
import sys
import urllib.error
import urllib.request

import numpy as np

BASE = "http://127.0.0.1:8050/app/nuki/"
DAY = "2026-10-01"

# The callback get_data_from_measurement in app/app.py: three outputs, three inputs
PAYLOAD = {
    "output": "..graph.children...table.rowData...table.columnDefs..",
    "outputs": [
        {"id": "graph", "property": "children"},
        {"id": "table", "property": "rowData"},
        {"id": "table", "property": "columnDefs"},
    ],
    "inputs": [
        {"id": "mast", "property": "value", "value": "Mast 6"},
        {"id": "date-range-picker", "property": "start_date", "value": DAY},
        {"id": "date-range-picker", "property": "end_date", "value": DAY},
    ],
    "changedPropIds": ["mast.value"],
    "state": [],
}

# Expected figure title -> (trace names, y values of the first trace), from the fixtures.
# The battery trace is named "B": app.py passes label_names="Battery level" (a string, not a
# list) and plot_measures takes label_names[0]. That is the app's current behaviour.
EXPECTED = {
    "Battery Level - Mast 6": (["B"], [3.71, 3.70, 3.69]),
    "Ground Temperature - Mast 6": (["-0.10 m", "-0.25 m", "-0.50 m"], [-1.5, -1.6, -1.7]),
    "Air Temperature - Mast 6": (["Air Temperature"], [-8.4, -8.9, -9.2]),
    "Relative Humidity - Mast 6": (["Relative Humidity"], [81.0, 83.5, 85.0]),
    "Barometric Pressure - Mast 6": (["Barometric Pressure"], [99.8, 99.7, 99.6]),
    "Inclination - Mast 6": (["InclX", "InclY"], [0.12, 0.13, 0.11]),
}


def fail(msg):
    print(msg)
    sys.exit(1)


def values(v):
    """Plotly 6 sends numeric arrays as {"dtype", "bdata"} (base64); plain lists otherwise."""
    if isinstance(v, dict) and "bdata" in v:
        return np.frombuffer(base64.b64decode(v["bdata"]), dtype=v["dtype"]).tolist()
    return list(v)


def figures(node):
    """All dcc.Graph figures in a Dash component tree (JSON)."""
    if isinstance(node, list):
        for item in node:
            yield from figures(item)
    elif isinstance(node, dict):
        if node.get("type") == "Graph" and "figure" in node.get("props", {}):
            yield node["props"]["figure"]
        for value in node.get("props", {}).values():
            yield from figures(value)


def title_of(fig):
    t = fig.get("layout", {}).get("title", {})
    return t.get("text") if isinstance(t, dict) else t


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


def check_callback():
    req = urllib.request.Request(
        BASE + "_dash-update-component", data=json.dumps(PAYLOAD).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            status, body = r.status, r.read()
    except urllib.error.HTTPError as e:
        fail(f"POST callback: HTTP {e.code}: {e.read()[:300]!r}")
    if status != 200:
        fail(f"POST callback: HTTP {status}")
    resp = json.loads(body)["response"]
    figs = {title_of(f): f for f in figures(resp["graph"]["children"])}
    if sorted(figs) != sorted(EXPECTED):
        fail(f"figure titles: expected {sorted(EXPECTED)}, got {sorted(figs)}")
    for title, (names, ys) in EXPECTED.items():
        data = figs[title].get("data", [])
        got_names = [t.get("name") for t in data]
        if got_names != names:
            fail(f"{title}: trace names {got_names}, expected {names} (texts: {[t.get('text') for t in data]})")
        got = [round(y, 6) for y in values(data[0]["y"])]
        if got != ys:
            fail(f"{title}: y values {got}, expected {ys}")
    rows = resp["table"]["rowData"]
    cols = [c["field"] for c in resp["table"]["columnDefs"]]
    if cols != ["GroundTemp01", "GroundTemp02", "GroundTemp03"] or len(rows) != 3:
        fail(f"table: columns {cols}, {len(rows)} rows")
    print(f"OK {len(figs)} figures from the stub data, table {len(rows)} rows")


if __name__ == "__main__":
    {"index": check_index, "callback": check_callback}.get(
        sys.argv[1] if len(sys.argv) > 1 else "", lambda: fail(__doc__))()
