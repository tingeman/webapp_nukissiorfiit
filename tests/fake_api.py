"""Minimal stand-in for the monitoring_db read API (v1 and v2), used by tests/run.sh and the
unit tests.

    python fake_api.py <fixtures-dir> [port] [host]

or in-process: make_server(fixtures_dir, port, host) returns a server (port 0: a free port,
see server.server_port) whose serve_forever() answers the requests.

The endpoints nuki uses are answered under any prefix (so http://host/api/v1/..., the
production form http://host/mdb/api/v1/... and the root form http://host/mdb/api/v2/... work):

  GET <prefix>/api/v1/metadata           <fixtures-dir>/metadata.json, narrowed by station/serial
  GET <prefix>/api/v2/metadata           the same plus first/last_measurement_time
  GET <prefix>/api/v2/measurements/raw   columnar data of <fixtures-dir>/measurements_v2.json
  GET <prefix>/api/v1/measurements       LEGACY: rows of <fixtures-dir>/measurements_v1.json
                                         ({"<deveui>": [row, ...]}), as the app used it before
                                         the move to v2; goes when nothing uses it any more

The fixtures are written by fixtures/make_fixtures.py (see its docstring for the stations,
devices, ids and periods). metadata.json is the v1 metadata shape; its timeseries carry the
fixture-only fields "_datatype" and "_unit" (not served in the metadata, like the real API;
used for the column details of v2). measurements_v2.json is {"<timeseries id>": [[time,
value], ...]}. The v1 measurements fixture is separate and older (3 hourly rows of
feedbeefcafe0002 on 2026-10-01, with the series names the app's v1 code and the smoke test
expect: InclX/InclY, pressure in kPa); it is not derived from the v2 data.

The shapes and rules follow monitoring_db (docs/read_api.md, app/monitoring_db/views.py):

* metadata: stations / recording_devices / sensors / timeseries / sensor_types. v2 adds
  first_measurement_time / last_measurement_time (YYYY-MM-DDTHH:MM:SSZ, null without data)
  to each recording device and timeseries. Parameters station and serial only; station
  matches the identifier, then the name (several name matches: 400 AMBIGUOUS_SELECTOR).
* v2 raw: one of deveui, serial (+ station when the serial is shared: else 400
  AMBIGUOUS_SELECTOR) or station alone (every device of the station). Whenever station is
  given (alone or with serial), the keys are prefixed "<device serial>__", or "device<id>__"
  without a serial, as in the real API (station_mode = bool(station)); start and end required,
  timezone-aware ISO 8601, inclusive, start <= end; filters sensor_name, sensor_name__in,
  sensor_type, timeseries_name, relative_z, relative_z_min, relative_z_max; calibrated and
  include_masked (true/false) are accepted, calibrated is echoed in meta and changes
  nothing (no calibrations in the fixtures), include_masked=true adds the "masked" arrays
  (all false). Response: request, time, columns (column details incl. key and calibration:
  null; only columns with data in the range, ordered by first appearance, then sensor name,
  series name, device id, series id), values (one array per column aligned with time, null
  where missing), meta. Gzip-compressed (Content-Encoding: gzip) when the request sends
  Accept-Encoding: gzip.
* errors: {"error": {"code", "message", "details"}}: 400 INVALID_QUERY / AMBIGUOUS_SELECTOR,
  404 DEVEUI_NOT_FOUND / SERIAL_NOT_FOUND / STATION_NOT_FOUND. 504 and 500 only via force.

Test controls (in-process only):
  server.requests   list of (path, {param: value}) received so far
  server.headers    list of {header name (lower case): value}, one per entry of requests
  server.force      None, or (status, body): every API request is answered with that
                    status and body (a dict is sent as JSON, bytes as they are; gzip
                    applies to the v2 measurement paths as usual)
  server.fixtures   the loaded fixtures ("metadata", "measurements" (v2: {timeseries id:
                    [(datetime, time string, value), ...]}), "measurements_v1"); a test
                    may replace parts of it
  QUERY_TIMEOUT     (504, body) of the real API's statement timeout, for server.force

Only the standard library is used, so it runs with the image's own python. From the
command line, each request is logged to stdout as "REQ <path>?<query>".
"""
import gzip
import json
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlparse

FILTER_PARAMS = {
    "sensor_name", "sensor_name__in", "sensor_type", "timeseries_name", "relative_z",
    "relative_z_min", "relative_z_max",
}
MEASUREMENT_PARAMS = {"deveui", "serial", "station", "start", "end", "include_masked"} | FILTER_PARAMS
METADATA_PARAMS = {"station", "serial"}

QUERY_TIMEOUT = (504, {"error": {"code": "QUERY_TIMEOUT", "message": "The database query timed out.",
                                 "details": {}}})


class ApiError(Exception):
    def __init__(self, status, code, message, details=None):
        super().__init__(message)
        self.response = _error(status, code, message, details)


def _error(status, code, message, details=None):
    return status, {"error": {"code": code, "message": message, "details": details or {}}}


def _parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iso(value):
    """UTC ISO 8601 with Z, as the real API's _to_utc_iso8601 (microseconds kept)."""
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_seconds(value):
    return None if value is None else value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _nulls_last(value):
    return (value is None, value or "")


# ---- v1 measurements (legacy) -------------------------------------------------------------

def measurements(fixtures, params):
    unknown = sorted(set(params) - MEASUREMENT_PARAMS)
    if unknown:
        return _error(400, "INVALID_QUERY", "Unsupported query parameters supplied.", {"params": unknown})
    deveui = params.get("deveui")
    if not deveui:
        return _error(400, "INVALID_QUERY", "deveui is required.")
    rows = fixtures["measurements_v1"].get(deveui)
    if rows is None:
        return _error(404, "DEVEUI_NOT_FOUND", "Recording device not found.", {"deveui": deveui})
    try:
        start = _parse_time(params["start"]) if params.get("start") else None
        end = _parse_time(params["end"]) if params.get("end") else None
    except ValueError:
        return _error(400, "INVALID_QUERY", "start and end must be ISO 8601 timestamps.")
    if start and end and start > end:
        return _error(400, "INVALID_QUERY", "start must be before end.")

    names = None
    if params.get("sensor_name"):
        names = {params["sensor_name"]}
    elif params.get("sensor_name__in"):
        names = {n for n in params["sensor_name__in"].split(",") if n}

    columns = ["timestamp"]
    data = []
    for row in sorted(rows, key=lambda r: r["timestamp"]):
        ts = _parse_time(row["timestamp"])
        if (start and ts < start) or (end and ts > end):
            continue
        out = {"timestamp": row["timestamp"]}
        for key, value in row.items():
            if key == "timestamp" or (names is not None and key.split("__", 1)[0] not in names):
                continue
            out[key] = value
            if key not in columns:
                columns.append(key)
        if len(out) > 1:
            data.append(out)
    details = [
        {"key": key, "sensor_name": key.split("__", 1)[0], "timeseries_name": key.split("__", 1)[1]}
        for key in columns[1:]
    ]
    return 200, {
        "request": {"deveui": deveui, "serial": None, "station": None,
                    "start": params.get("start"), "end": params.get("end"),
                    "filters": {"sensor_name": params.get("sensor_name"),
                                "sensor_name__in": params.get("sensor_name__in")}},
        "columns": columns,
        "data": data,
        "column_details": details,
        "meta": {"row_count": len(data), "deduplicated": True, "sorted": "timestamp_asc",
                 "timezone": "UTC", "column_key_source": "canonical_db_fields",
                 "column_key_pattern": "<sensor.name>__<timeseries.name>"},
    }


# ---- device resolution (shared by metadata and v2 measurements) ---------------------------

def resolve_devices(meta, deveui=None, serial=None, station=None):
    """(list of device dicts, station dict or None), with the real API's rules and errors."""
    devices = sorted(meta.get("recording_devices") or [], key=lambda d: d["id"])
    stations = sorted(meta.get("stations") or [], key=lambda s: s["id"])
    if deveui:
        found = [d for d in devices if d.get("deveui") == deveui]
        if not found:
            raise ApiError(404, "DEVEUI_NOT_FOUND", "Recording device not found.", {"deveui": deveui})
        return found[:1], next((s for s in stations if s["id"] == found[0].get("station_id")), None)

    station_obj = None
    if station:
        station_obj = next((s for s in stations if s.get("identifier") == station), None)
        if station_obj is None:
            matches = [s for s in stations if s.get("name") == station]
            if len(matches) > 1:
                raise ApiError(400, "AMBIGUOUS_SELECTOR", "station matches several stations by name.",
                               {"station": station, "stations": [m["identifier"] for m in matches]})
            station_obj = matches[0] if matches else None
        if station_obj is None:
            raise ApiError(404, "STATION_NOT_FOUND", "Station not found.", {"station": station})

    if serial:
        found = [d for d in devices if d.get("serial") == serial]
        if station_obj is not None:
            found = [d for d in found if d.get("station_id") == station_obj["id"]]
        if not found:
            details = {"serial": serial}
            if station:
                details["station"] = station
            raise ApiError(404, "SERIAL_NOT_FOUND", "Recording device not found.", details)
        if len(found) > 1:
            idents = {d.get("station_identifier") for d in found}
            raise ApiError(400, "AMBIGUOUS_SELECTOR",
                           "serial matches several recording devices; add the station parameter.",
                           {"serial": serial, "stations": sorted(idents, key=_nulls_last)})
        return found, next((s for s in stations if s["id"] == found[0].get("station_id")), None)

    return [d for d in devices if d.get("station_id") == station_obj["id"]], station_obj


# ---- metadata -----------------------------------------------------------------------------

def _public(item):
    return {k: v for k, v in item.items() if not k.startswith("_")}


def _metadata(fixtures, params, include_times):
    unknown = sorted(set(params) - METADATA_PARAMS)
    if unknown:
        return _error(400, "INVALID_QUERY", "Unsupported query parameters supplied.", {"params": unknown})
    meta = fixtures["metadata"]
    stations = [dict(s) for s in meta.get("stations") or []]
    devices = [dict(d) for d in meta.get("recording_devices") or []]
    sensors = [dict(s) for s in meta.get("sensors") or []]
    series = [_public(t) for t in meta.get("timeseries") or []]
    station_param = params.get("station") or None
    serial_param = params.get("serial") or None
    if station_param or serial_param:
        try:
            found, station_obj = resolve_devices(meta, None, serial_param, station_param)
        except ApiError as exc:
            return exc.response
        ids = {d["id"] for d in found}
        devices = [d for d in devices if d["id"] in ids]
        sensors = [s for s in sensors if s.get("recording_device_id") in ids]
        sensor_ids = {s["id"] for s in sensors}
        series = [t for t in series if t.get("sensor_id") in sensor_ids]
        stations = [s for s in stations if station_obj and s["id"] == station_obj["id"]]
    if include_times:
        data = fixtures["measurements"]
        device_of = {s["id"]: s.get("recording_device_id") for s in meta.get("sensors") or []}
        device_times = {}
        for ts in series:
            points = data.get(ts["id"]) or []
            first = points[0][0] if points else None
            last = points[-1][0] if points else None
            ts["first_measurement_time"] = _iso_seconds(first)
            ts["last_measurement_time"] = _iso_seconds(last)
            if first is None:
                continue
            dev = device_of.get(ts.get("sensor_id"))
            lo, hi = device_times.get(dev, (first, last))
            device_times[dev] = (min(lo, first), max(hi, last))
        for d in devices:
            lo, hi = device_times.get(d["id"], (None, None))
            d["first_measurement_time"] = _iso_seconds(lo)
            d["last_measurement_time"] = _iso_seconds(hi)
    return 200, {
        "stations": stations,
        "recording_devices": devices,
        "sensors": sensors,
        "timeseries": series,
        "sensor_types": sorted({s["type"] for s in sensors if s.get("type")}),
    }


def metadata(fixtures, params):
    return _metadata(fixtures, params, include_times=False)


def metadata_v2(fixtures, params):
    return _metadata(fixtures, params, include_times=True)


# ---- v2 raw measurements ------------------------------------------------------------------

def _bool_param(params, name):
    if name not in params:
        return None
    raw = params[name].strip().lower()
    if raw not in ("true", "false"):
        raise ApiError(400, "INVALID_QUERY", f"{name} must be true or false.")
    return raw == "true"


def _datetime_param(params, name):
    value = params[name]
    try:
        if "T" not in value and " " not in value:
            raise ValueError(value)
        parsed = _parse_time(value)
    except ValueError:
        raise ApiError(400, "INVALID_QUERY", f"{name} must be a valid ISO 8601 datetime.") from None
    if parsed.tzinfo is None:
        raise ApiError(400, "INVALID_QUERY", f"{name} must include a timezone offset.")
    return parsed.astimezone(timezone.utc)


def _float_param(params, name):
    if name not in params:
        return None
    try:
        return float(params[name])
    except ValueError:
        raise ApiError(400, "INVALID_QUERY", f"{name} must be numeric.") from None


def parse_measurement_request(params):
    """The real API's _parse_measurement_request(allow_calibrated=True), as a dict."""
    unknown = sorted(set(params) - MEASUREMENT_PARAMS - {"calibrated"})
    if unknown:
        raise ApiError(400, "INVALID_QUERY", "Unsupported query parameters supplied.", {"params": unknown})
    calibrated = _bool_param(params, "calibrated")
    include_masked = _bool_param(params, "include_masked")
    deveui = params.get("deveui") or None
    serial = params.get("serial") or None
    station = params.get("station") or None
    if not (deveui or serial or station) or not params.get("start") or not params.get("end"):
        raise ApiError(400, "INVALID_QUERY",
                       "One of deveui, serial or station, plus start and end, are required query parameters.")
    if deveui and (serial or station):
        raise ApiError(400, "INVALID_QUERY", "deveui cannot be combined with serial or station.")
    start = _datetime_param(params, "start")
    end = _datetime_param(params, "end")
    if start > end:
        raise ApiError(400, "INVALID_QUERY", "start must be less than or equal to end.")
    relative_z = _float_param(params, "relative_z")
    z_min = _float_param(params, "relative_z_min")
    z_max = _float_param(params, "relative_z_max")
    if relative_z is not None and (z_min is not None or z_max is not None):
        raise ApiError(400, "INVALID_QUERY", "relative_z cannot be combined with relative_z_min or relative_z_max.")
    names_in = None
    if params.get("sensor_name__in") is not None:
        names_in = [n.strip() for n in params["sensor_name__in"].split(",") if n.strip()]
        if not names_in:
            raise ApiError(400, "INVALID_QUERY", "sensor_name__in must contain at least one value.")
    return {
        "calibrated": True if calibrated is None else calibrated,
        "include_masked": bool(include_masked),
        "deveui": deveui, "serial": serial, "station": station, "start": start, "end": end,
        "sensor_name": params.get("sensor_name"), "sensor_name__in": names_in,
        "sensor_type": params.get("sensor_type"), "timeseries_name": params.get("timeseries_name"),
        "relative_z": relative_z, "relative_z_min": z_min, "relative_z_max": z_max,
    }


def _sensor_matches(sensor, series, f):
    z = sensor.get("relative_z")
    return not (
        (f["sensor_name"] and sensor["name"] != f["sensor_name"])
        or (f["sensor_name__in"] and sensor["name"] not in f["sensor_name__in"])
        or (f["sensor_type"] and sensor.get("type") != f["sensor_type"])
        or (f["timeseries_name"] and series["name"] != f["timeseries_name"])
        or (f["relative_z"] is not None and z != f["relative_z"])
        or (f["relative_z_min"] is not None and (z is None or z < f["relative_z_min"]))
        or (f["relative_z_max"] is not None and (z is None or z > f["relative_z_max"]))
    )


def measurements_raw(fixtures, params):
    try:
        f = parse_measurement_request(params)
        meta = fixtures["metadata"]
        devices, _station = resolve_devices(meta, f["deveui"], f["serial"], f["station"])
    except ApiError as exc:
        return exc.response
    station_mode = bool(f["station"])
    device_by_id = {d["id"]: d for d in devices}
    sensors = {s["id"]: s for s in meta.get("sensors") or [] if s.get("recording_device_id") in device_by_id}

    # Per timeseries with data in the range: its points, first time and column details
    found = []
    for ts in meta.get("timeseries") or []:
        sensor = sensors.get(ts.get("sensor_id"))
        if sensor is None or not _sensor_matches(sensor, ts, f):
            continue
        points = [(when, value) for when, _s, value in fixtures["measurements"].get(ts["id"]) or []
                  if f["start"] <= when <= f["end"]]
        if not points:
            continue
        device = device_by_id[sensor["recording_device_id"]]
        key = f"{sensor['name']}__{ts['name']}"
        if station_mode:
            key = f"{device.get('serial') or 'device%d' % device['id']}__{key}"
        detail = {
            "key": key,
            "recording_device_id": device["id"],
            "recording_device_serial": device.get("serial"),
            "sensor_id": sensor["id"],
            "sensor_name": sensor["name"],
            "sensor_type": sensor.get("type"),
            "relative_z": sensor.get("relative_z"),
            "timeseries_id": ts["id"],
            "timeseries_name": ts["name"],
            "datatype": ts.get("_datatype"),
            "unit": ts.get("_unit"),
            "calibration": None,
        }
        rank = (sensor["name"], ts["name"], device["id"], ts["id"])
        found.append((points[0][0], rank, key, detail, dict(points)))

    found.sort(key=lambda x: (x[0], x[1]))
    axis = sorted({when for *_rest, points in found for when in points})
    # Same key: one column; position and details of the first, the last-ranked series wins
    columns, merged = [], {}
    for _first, rank, key, detail, points in found:
        if key not in merged:
            columns.append(detail)
            merged[key] = []
        merged[key].append((rank, points))
    values = []
    for detail in columns:
        members = sorted(merged[detail["key"]], key=lambda m: m[0], reverse=True)
        values.append([next((p[when] for _r, p in members if when in p), None) for when in axis])

    payload = {
        "request": {
            "deveui": f["deveui"], "serial": f["serial"], "station": f["station"],
            "start": _iso(f["start"]), "end": _iso(f["end"]),
            "filters": {k: f[k] for k in ("sensor_name", "sensor_name__in", "sensor_type", "timeseries_name",
                                          "relative_z", "relative_z_min", "relative_z_max")},
        },
        "time": [_iso(when) for when in axis],
        "columns": columns,
        "values": values,
        "meta": {
            "row_count": len(axis), "column_count": len(columns), "format": "columnar",
            "resolution": "raw", "sorted": "timestamp_asc", "timezone": "UTC",
            "column_key_source": "canonical_db_fields",
            "column_key_pattern": ("<device>__<sensor.name>__<timeseries.name>" if station_mode
                                   else "<sensor.name>__<timeseries.name>"),
            "calibrated": f["calibrated"],
        },
    }
    if f["include_masked"]:
        payload["masked"] = [[None if v is None else False for v in col] for col in values]
    return 200, payload


# ---- server -------------------------------------------------------------------------------

# (path suffix, handler, gzip when accepted)
ROUTES = [
    ("/api/v1/metadata", metadata, False),
    ("/api/v2/metadata", metadata_v2, False),
    ("/api/v1/measurements", measurements, False),
    ("/api/v2/measurements/raw", measurements_raw, True),
]


def accepts_gzip(header):
    for part in (header or "").split(","):
        token, _, q = part.strip().partition(";")
        if token.strip().lower() == "gzip":
            return q.strip().replace(" ", "") not in ("q=0", "q=0.0", "q=0.00", "q=0.000")
    return False


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # quiet; the command line logs REQ lines instead
        pass

    def _send(self, status, body, compress=False):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        if compress:
            raw = gzip.compress(raw)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Vary", "Accept-Encoding")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        url = urlparse(self.path)
        params = dict(parse_qsl(url.query, keep_blank_values=True))
        srv = self.server
        srv.requests.append((url.path, params))
        srv.headers.append({k.lower(): v for k, v in self.headers.items()})
        if srv.verbose:
            print(f"REQ {self.path}", flush=True)
        route = next((r for r in ROUTES if url.path.endswith(r[0])), None)
        if route is None:
            self._send(404, b"not found")
            return
        _suffix, handler, may_gzip = route
        compress = may_gzip and accepts_gzip(self.headers.get("Accept-Encoding"))
        if srv.force is not None:
            self._send(*srv.force, compress=compress)
            return
        status, body = handler(srv.fixtures, params)
        self._send(status, body, compress=compress)


def load_fixtures(fixtures_dir):
    d = Path(fixtures_dir)
    raw = json.loads((d / "measurements_v2.json").read_text(encoding="utf-8"))
    return {
        "metadata": json.loads((d / "metadata.json").read_text(encoding="utf-8")),
        # {timeseries id: [(datetime, time string, value), ...]}, ascending
        "measurements": {int(ts): sorted((_parse_time(when), when, value) for when, value in points)
                         for ts, points in raw.items()},
        "measurements_v1": json.loads((d / "measurements_v1.json").read_text(encoding="utf-8")),
    }


def make_server(fixtures_dir, port=0, host="127.0.0.1", verbose=False):
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    server.fixtures = load_fixtures(fixtures_dir)
    server.requests = []
    server.headers = []
    server.force = None
    server.verbose = verbose
    return server


def main(argv):
    if not argv or len(argv) > 3:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 2
    port = int(argv[1]) if len(argv) > 1 else 8099
    host = argv[2] if len(argv) > 2 else "0.0.0.0"
    server = make_server(argv[0], port, host, verbose=True)
    print(f"fake_api listening on {host}:{server.server_port}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
