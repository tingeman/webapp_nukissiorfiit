"""Minimal stand-in for the monitoring_db read API (/api/v1), used by tests/run.sh and the
unit tests.

    python fake_api.py <fixtures-dir> [port] [host]

or in-process: make_server(fixtures_dir, port, host) returns a server (port 0: a free port,
see server.server_port) whose serve_forever() answers the requests.

Only the two endpoints nuki uses are answered, under any prefix (so both
http://host/api/v1 and the production form http://host/mdb/api/v1 work):

  GET <prefix>/api/v1/metadata        <fixtures-dir>/metadata.json
  GET <prefix>/api/v1/measurements    rows of <fixtures-dir>/measurements.json
                                      ({"<deveui>": [row, ...]}), filtered like the real API

The response shapes follow monitoring_db (app/monitoring_db/views.py): metadata has
stations / recording_devices / sensors / timeseries / sensor_types; measurements has
request / columns / data / column_details / meta, the columns keyed
"<sensor.name>__<timeseries.name>" (device mode), the rows {"timestamp": "...Z", <key>:
value, ...} sorted by time. Errors are {"error": {"code", "message", "details"}} with the
real status codes: 400 (INVALID_QUERY), 404 (DEVEUI_NOT_FOUND).

Test controls (in-process only):
  server.requests   list of (path, {param: value}) received so far
  server.force      None, or (status, body): every API request is answered with that
                    status and body (a dict is sent as JSON, bytes as they are)

Only the standard library is used, so it runs with the image's own python. From the
command line, each request is logged to stdout as "REQ <path>?<query>".
"""
import json
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlparse

MEASUREMENT_PARAMS = {
    "deveui", "serial", "station", "start", "end", "sensor_name", "sensor_name__in",
    "sensor_type", "timeseries_name", "relative_z", "relative_z_min", "relative_z_max",
    "include_masked",
}
METADATA_PARAMS = {"station", "serial"}


def _error(status, code, message, details=None):
    return status, {"error": {"code": code, "message": message, "details": details or {}}}


def _parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def measurements(fixtures, params):
    unknown = sorted(set(params) - MEASUREMENT_PARAMS)
    if unknown:
        return _error(400, "INVALID_QUERY", "Unsupported query parameters supplied.", {"params": unknown})
    deveui = params.get("deveui")
    if not deveui:
        return _error(400, "INVALID_QUERY", "deveui is required.")
    rows = fixtures["measurements"].get(deveui)
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


def metadata(fixtures, params):
    unknown = sorted(set(params) - METADATA_PARAMS)
    if unknown:
        return _error(400, "INVALID_QUERY", "Unsupported query parameters supplied.", {"params": unknown})
    return 200, fixtures["metadata"]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # quiet; the command line logs REQ lines instead
        pass

    def _send(self, status, body):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        url = urlparse(self.path)
        params = dict(parse_qsl(url.query, keep_blank_values=True))
        srv = self.server
        srv.requests.append((url.path, params))
        if srv.verbose:
            print(f"REQ {self.path}", flush=True)
        if url.path.endswith("/api/v1/metadata"):
            handler = metadata
        elif url.path.endswith("/api/v1/measurements"):
            handler = measurements
        else:
            self._send(404, b"not found")
            return
        if srv.force is not None:
            self._send(*srv.force)
            return
        self._send(*handler(srv.fixtures, params))


def load_fixtures(fixtures_dir):
    d = Path(fixtures_dir)
    return {
        "metadata": json.loads((d / "metadata.json").read_text(encoding="utf-8")),
        "measurements": json.loads((d / "measurements.json").read_text(encoding="utf-8")),
    }


def make_server(fixtures_dir, port=0, host="127.0.0.1", verbose=False):
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    server.fixtures = load_fixtures(fixtures_dir)
    server.requests = []
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
