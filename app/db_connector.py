"""All data access of the app: the monitoring_db read API over HTTP.

API v2 (the phase 1 page):

    mast_catalogue()                    -> [{"name", "station_identifiers", "loggers"}], by mast number
    default_logger(mast)                -> the logger of a mast with the latest data
    get_logger_data(value, start, stop) -> LoggerData(frame, columns), every series of one logger
    battery(data), ground_temperature(data), inclination(data) -> (frame, labels)
    weather(data, "AirTemp" | "RelHum" | "BarometricPressure") -> (frame, labels, unit)

and the *_async wrappers of these for the Dash callbacks.

Requests: GET <root>/api/v1/metadata (whole database, cached 10 minutes per process),
GET <root>/api/v2/metadata?station=<identifier> (cached 10 minutes per station) and
GET <root>/api/v2/measurements/raw (split into chunks of BACKEND_API_RAW_CHUNK_YEARS years,
at most BACKEND_API_MAX_PARALLEL in flight). <root> is BACKEND_API_BASE_URL without a trailing
"/", "/api/v1" or "/api/v2". Only connection errors are retried (BACKEND_API_MAX_RETRIES); a
504 or a read timeout raises QueryTimeout at once.

Nothing but plain values and locks is created at import: gunicorn imports the app with
--preload and then forks, so sessions and thread pools are made per call.

The v1 measurement functions (get_sensor_data, get_ground_temp,
get_ground_temp_sensor_depths) are what the page uses until T3 switches it to v2.
"""
import logging
import re
import threading
import time
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asgiref.sync
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import settings

logger = logging.getLogger(__name__)

METADATA_TTL_S = 600.0
_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_monotonic = time.monotonic    # patched by the tests


class EmptyQuerySet(Exception):
    pass


class RecordingDeviceNotFound(Exception):
    pass


class SensorNotFound(Exception):
    pass


class QueryTimeout(Exception):
    pass


# ---- transport --------------------------------------------------------------------------------

_API_SUFFIX = re.compile(r"/api/v[12]$")
_warned_base_urls = set()
_warn_lock = threading.Lock()


def _root_url():
    """BACKEND_API_BASE_URL without a trailing "/" and "/api/v1" or "/api/v2" (one WARNING
    per configured value when a suffix is stripped)."""
    configured = settings.backend_api_base_url
    root = configured.rstrip("/")
    stripped = _API_SUFFIX.sub("", root)
    if stripped != root:
        with _warn_lock:
            first = configured not in _warned_base_urls
            _warned_base_urls.add(configured)
        if first:
            logger.warning("BACKEND_API_BASE_URL %s ends with an API version; using the root %s "
                           "(set the root form to silence this warning)", configured, stripped)
    return stripped.rstrip("/")


def _build_session():
    # R1: connection errors are retried (the request never reached the API); a read timeout
    # (read=False: re-raised as requests' ReadTimeout) and any HTTP status (no
    # status_forcelist) are not, so a 504 or a slow query costs the API one request.
    retry = Retry(
        total=settings.backend_api_max_retries,
        connect=settings.backend_api_max_retries,
        read=False,
        status=0,
        other=0,
        backoff_factor=settings.backend_api_backoff_factor,
        allowed_methods=["GET"],
        respect_retry_after_header=False,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _api_get(path, params):
    """JSON of GET <root>/<path> (path e.g. "api/v2/metadata"), errors mapped to the module's
    exceptions: 404 RecordingDeviceNotFound, 504 or timeout QueryTimeout, 400 ValueError
    (the API's message), other >= 500, transport errors and invalid JSON RuntimeError."""
    url = f"{_root_url()}/{path.lstrip('/')}"

    try:
        with _build_session() as session:
            response = session.get(url, params=params, timeout=settings.backend_api_timeout_seconds)
    except requests.Timeout as exc:
        raise QueryTimeout("Backend API request timed out") from exc
    except requests.RequestException as exc:
        raise RuntimeError(f"Backend API transport error: {exc}") from exc

    if response.status_code == 404:
        raise RecordingDeviceNotFound("RecordingDevice not found in backend API")
    if response.status_code == 504:
        raise QueryTimeout("Backend API request timed out")
    if response.status_code == 400:
        message = "Invalid backend API request"
        try:
            message = response.json().get("error", {}).get("message", message)
        except (ValueError, AttributeError):
            pass
        raise ValueError(message)
    if response.status_code >= 500:
        raise RuntimeError(f"Backend API internal error ({response.status_code})")

    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError("Backend API returned invalid JSON") from exc


# ---- dates ------------------------------------------------------------------------------------

def _normalize_datetime_for_api(value, is_end=False):
    if value is None or value == "":
        return None

    # DatePickerRange commonly provides YYYY-MM-DD; map to an inclusive UTC interval.
    if isinstance(value, str) and len(value) == 10 and value.count("-") == 2:
        dt = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
        if is_end:
            dt = dt + timedelta(days=1) - timedelta(milliseconds=1)
        return dt.isoformat().replace("+00:00", "Z")

    parsed = pd.to_datetime(value, errors="raise", utc=False)
    if isinstance(parsed, pd.Timestamp):
        ts = parsed
    else:
        ts = pd.Timestamp(parsed)

    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")

    return ts.to_pydatetime().isoformat().replace("+00:00", "Z")


def _normalize_range_for_api(start, stop):
    end_iso = _normalize_datetime_for_api(stop, is_end=True)
    start_iso = _normalize_datetime_for_api(start, is_end=False)

    if end_iso is None:
        end_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if start_iso is None:
        start_iso = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat().replace("+00:00", "Z")

    return start_iso, end_iso


def _parse_utc(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _add_years(when, years):
    try:
        return when.replace(year=when.year + years)
    except ValueError:      # 29 February -> 1 March
        return when.replace(year=when.year + years, month=3, day=1)


def chunks(first, last, years, now=None):
    """(start, end) request parameters (YYYY-MM-DDTHH:MM:SSZ) covering first..last (UTC
    datetimes, both inclusive) in pieces of ``years`` years, counted from ``first``.
    Consecutive pieces touch without overlap (end = next start - 1 s); the last piece ends at
    ``last`` or ``now``, whichever is earlier. Empty if the period lies after ``now``."""
    if years < 1:
        raise ValueError("years must be 1 or more")
    now = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    first = first.replace(microsecond=0)
    last = min(last.replace(microsecond=0), now)
    out = []
    lo = first
    while lo <= last:
        boundary = _add_years(lo, years)
        hi = min(boundary - timedelta(seconds=1), last)
        out.append((lo.strftime(_TS_FORMAT), hi.strftime(_TS_FORMAT)))
        lo = boundary
    return out


# ---- metadata ---------------------------------------------------------------------------------

_cache_lock = threading.Lock()
_v1_metadata = {}      # root URL -> (monotonic time, JSON)
_v2_metadata = {}      # (root URL, station identifier) -> (monotonic time, JSON)


def clear_caches():
    """Forget the cached metadata (for the tests)."""
    with _cache_lock:
        _v1_metadata.clear()
        _v2_metadata.clear()


def _cached(cache, key, fetch):
    with _cache_lock:
        hit = cache.get(key)
        if hit is not None and _monotonic() - hit[0] < METADATA_TTL_S:
            return hit[1]
    data = fetch()      # outside the lock; failures raise and are not cached
    if not isinstance(data, dict):
        raise RuntimeError("Backend API metadata is not a JSON object")
    with _cache_lock:
        cache[key] = (_monotonic(), data)
    return data


def metadata_v1():
    """GET /api/v1/metadata (whole database), cached per process for 10 minutes."""
    root = _root_url()
    return _cached(_v1_metadata, root, lambda: _api_get("api/v1/metadata", params={}))


def metadata_v2(station):
    """GET /api/v2/metadata?station=<identifier>, cached per station for 10 minutes."""
    root = _root_url()
    return _cached(_v2_metadata, (root, station),
                   lambda: _api_get("api/v2/metadata", params={"station": station}))


# ---- mast catalogue ---------------------------------------------------------------------------

_MAST_PREFIX = "Nukissiorfiit "


def _mast_label(name):
    label = name[len(_MAST_PREFIX):] if name.startswith(_MAST_PREFIX) else name
    return label.strip() or name


def _mast_sort_key(label):
    m = re.search(r"\d+", label)
    return (0, int(m.group()), label) if m else (1, 0, label)


def _logger_entry(station, device):
    serial = (device.get("serial") or "").strip()
    deveui = (device.get("deveui") or "").strip()
    if serial:
        value, ident = f"serial:{station}:{serial}", serial
    elif deveui:
        value, ident = f"deveui:{deveui}", deveui
    else:
        return None
    first = device.get("first_measurement_time")
    last = device.get("last_measurement_time")
    display = device.get("display_name") or "Logger"
    label = f"{display} ({ident}), {(first or '')[:10]} to {last[:10]}"
    return {"value": value, "label": label, "station": station, "first": first, "last": last}


def mast_catalogue():
    """The Nukissiorfiit masts with their loggers, sorted by mast number:

        [{"name": "Mast 6", "station_identifiers": ["SIS2024-01"],
          "loggers": [{"value": "deveui:..." | "serial:<station>:<serial>",
                       "label": "<display name> (<serial or deveui>), <first> to <last>",
                       "station": "<identifier>",
                       "first": "YYYY-MM-DDTHH:MM:SSZ", "last": "YYYY-MM-DDTHH:MM:SSZ"}, ...]}]

    Stations: identifier fully matches NUKI_STATION_PATTERN, grouped by station name. Loggers
    (by first measurement) come from the v2 metadata of each station; loggers without
    measurements, and masts without such a logger, are left out."""
    pattern = settings.station_regex
    meta = metadata_v1()
    groups = {}
    for station in meta.get("stations") or []:
        ident = station.get("identifier")
        if not ident or not pattern.fullmatch(ident):
            continue
        name = station.get("name") or ident
        groups.setdefault(name, []).append(ident)

    masts = []
    for name, idents in groups.items():
        loggers = []
        for ident in idents:
            for device in metadata_v2(ident).get("recording_devices") or []:
                if not device.get("last_measurement_time"):
                    continue
                entry = _logger_entry(ident, device)
                if entry is not None:
                    loggers.append(entry)
        if not loggers:
            continue
        loggers.sort(key=lambda lg: (lg["first"] or "", lg["value"]))
        masts.append({"name": _mast_label(name), "station_identifiers": idents, "loggers": loggers})
    masts.sort(key=lambda m: _mast_sort_key(m["name"]))
    return masts


def default_logger(mast):
    """The logger of a catalogue mast with the latest last measurement (None if it has none)."""
    loggers = mast.get("loggers") or []
    if not loggers:
        return None
    return max(loggers, key=lambda lg: _parse_utc(lg["last"]))


# ---- measurements -----------------------------------------------------------------------------

@dataclass(frozen=True)
class LoggerData:
    """Every series of one logger: frame has a UTC DatetimeIndex and one float column per API
    key (NaN where missing); columns maps each key to its column details from the API
    (sensor_name, sensor_type, timeseries_name, datatype, unit, relative_z, ...)."""
    frame: pd.DataFrame
    columns: dict


def _logger_params(logger_value):
    kind, _, rest = (logger_value or "").partition(":")
    if kind == "deveui" and rest:
        return {"deveui": rest}
    if kind == "serial":
        station, _, serial = rest.partition(":")
        if station and serial:
            return {"station": station, "serial": serial}
    raise ValueError(f"Invalid logger: {logger_value!r}")


def _raw_chunk(params, start, end):
    payload = _api_get("api/v2/measurements/raw", params={**params, "start": start, "end": end})
    if not isinstance(payload, dict):
        raise RuntimeError("Backend API raw answer is not a JSON object")
    times, columns, values = payload.get("time"), payload.get("columns"), payload.get("values")
    if (not isinstance(times, list) or not isinstance(columns, list) or not isinstance(values, list)
            or len(values) != len(columns)
            or not all(isinstance(c, dict) and c.get("key") for c in columns)
            or not all(isinstance(v, list) and len(v) == len(times) for v in values)):
        raise RuntimeError("Backend API raw answer is not one array per column aligned with time")
    return payload


def _fetch_chunks(params, pieces):
    """The raw payloads of the pieces, in their order, at most backend_api_max_parallel at
    once; the first error is raised and the queued requests are dropped."""
    if len(pieces) == 1:
        return [_raw_chunk(params, *pieces[0])]
    pool = ThreadPoolExecutor(max_workers=min(settings.backend_api_max_parallel, len(pieces)),
                              thread_name_prefix="mdb-api")
    try:
        futures = [pool.submit(_raw_chunk, params, s, e) for s, e in pieces]
        done, _ = wait(futures, return_when=FIRST_EXCEPTION)
        for f in futures:
            if f in done and f.exception() is not None:
                raise f.exception()
        return [f.result() for f in futures]
    finally:
        # after an error: drop the queued chunks, do not wait for running ones
        pool.shutdown(wait=False, cancel_futures=True)


def _frame_of(payloads):
    parts, details = [], {}
    for payload in payloads:
        if not payload["time"]:
            continue
        keys = [c["key"] for c in payload["columns"]]
        for c in payload["columns"]:
            details.setdefault(c["key"], dict(c))
        index = pd.to_datetime(payload["time"], utc=True)
        parts.append(pd.DataFrame(dict(zip(keys, payload["values"])), index=index, columns=keys))
    if not parts:
        raise EmptyQuerySet("No data found for the given query")
    frame = pd.concat(parts, axis=0) if len(parts) > 1 else parts[0]
    try:
        frame = frame.astype(float)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Backend API returned non-numeric values") from exc
    frame.index.name = "timestamp"
    frame = frame.sort_index(kind="stable")
    frame = frame[~frame.index.duplicated(keep="first")]
    frame = frame.reindex(columns=list(details))
    return LoggerData(frame=frame, columns=details)


def get_logger_data(logger_value, start, stop):
    """Every series of one logger (a catalogue logger value) from start to stop: date strings
    YYYY-MM-DD (whole days, inclusive) or datetimes (UTC if naive); None: the last 30 days up
    to now. Returns LoggerData. Raises EmptyQuerySet (no rows), RecordingDeviceNotFound (404),
    QueryTimeout (504, timeout), ValueError (bad logger value, 400), RuntimeError (other)."""
    params = _logger_params(logger_value)
    start_iso, end_iso = _normalize_range_for_api(start, stop)
    first, last = _parse_utc(start_iso), _parse_utc(end_iso)
    if first > last:
        raise ValueError("start must be before stop")
    pieces = chunks(first, last, settings.backend_api_raw_chunk_years)
    if not pieces:
        raise EmptyQuerySet("No data found for the given query")
    return _frame_of(_fetch_chunks(params, pieces))


# ---- selections (no HTTP) ---------------------------------------------------------------------

def _select(data, keys, labels):
    if not keys:
        raise EmptyQuerySet("No data found for the given query")
    return data.frame[keys], labels


def battery(data):
    """(frame, labels) of the BatteryVoltage series."""
    keys = [k for k, c in data.columns.items() if c.get("timeseries_name") == "BatteryVoltage"]
    return _select(data, keys, [data.columns[k]["timeseries_name"] for k in keys])


def ground_temperature(data):
    """(frame, labels) of the GroundTemperature series with relative_z <= 0, shallow to deep,
    labelled by depth ("-0.10 m")."""
    keys = [k for k, c in data.columns.items()
            if c.get("timeseries_name") == "GroundTemperature"
            and c.get("relative_z") is not None and c["relative_z"] <= 0]
    keys.sort(key=lambda k: -data.columns[k]["relative_z"])
    return _select(data, keys, [f"{data.columns[k]['relative_z']:+0.2f} m" for k in keys])


WEATHER_SERIES = ("AirTemp", "RelHum", "BarometricPressure")
_TO_KPA = {"Pa": 0.001, "hPa": 0.1, "kPa": 1.0}


def weather(data, name):
    """(frame, labels, unit) of one weather series ("AirTemp", "RelHum" or
    "BarometricPressure"). Pressure is converted to kPa by the column's unit (Pa, hPa, kPa);
    an unknown unit is returned unconverted with that unit, and a WARNING is logged."""
    if name not in WEATHER_SERIES:
        raise ValueError(f"Unknown weather series: {name!r}")
    keys = [k for k, c in data.columns.items() if c.get("timeseries_name") == name]
    frame, labels = _select(data, keys, [name for _ in keys])
    units = []
    if name == "BarometricPressure":
        frame = frame.copy()
        for k in keys:
            unit = data.columns[k].get("unit")
            factor = _TO_KPA.get(unit)
            if factor is None:
                logger.warning("BarometricPressure column %s has unit %r, not Pa/hPa/kPa: not converted", k, unit)
            else:
                frame[k] = frame[k] * factor
                unit = "kPa"
            units.append(unit)
    else:
        units = [data.columns[k].get("unit") for k in keys]
    distinct = list(dict.fromkeys(u for u in units))
    unit = distinct[0] if len(distinct) == 1 else ", ".join(str(u) for u in distinct)
    return frame, labels, unit


def inclination(data):
    """(frame, labels) of the series with datatype Angle (Tekbox Roll/Pitch, Campbell X/Y),
    labelled by series name."""
    keys = [k for k, c in data.columns.items() if c.get("datatype") == "Angle"]
    return _select(data, keys, [data.columns[k]["timeseries_name"] for k in keys])


# ---- v1 (legacy, until T3 switches the page) --------------------------------------------------

def _build_dataframe_from_measurements(payload):
    rows = payload.get("data", [])
    if not rows:
        raise EmptyQuerySet("No data found for the given query")

    frame = pd.DataFrame(rows)
    if "timestamp" not in frame.columns:
        raise RuntimeError("Backend API response missing timestamp column")

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame.set_index("timestamp", inplace=True)
    frame.sort_index(inplace=True)
    frame.drop_duplicates(inplace=True)
    return frame


def _to_timeseries_columns(frame):
    renamed = {}
    for col in frame.columns:
        if "__" in col:
            renamed[col] = col.split("__", 1)[1]
        else:
            renamed[col] = col

    frame = frame.rename(columns=renamed)
    # Preserve deterministic order while removing duplicated names.
    frame = frame.loc[:, ~frame.columns.duplicated()]
    return frame


def _get_metadata_payload():
    return _api_get("api/v1/metadata", params={})


def _get_ground_temp_sensor_names(deveui):
    payload = _get_metadata_payload()
    sensor_names = [
        sensor.get("name")
        for sensor in payload.get("sensors", [])
        if sensor.get("recording_device_deveui") == deveui and sensor.get("name") and "groundtemp" in sensor["name"].lower()
    ]
    sensor_names = sorted(set(sensor_names))
    return sensor_names


def close_all_stale_db_connections():
    # Kept for backward compatibility; no-op on REST path.
    return None


def get_sensor_data(deveui, sensor_name, start, stop):
    close_all_stale_db_connections()
    start_iso, end_iso = _normalize_range_for_api(start, stop)

    payload = _api_get(
        "api/v1/measurements",
        params={
            "deveui": deveui,
            "sensor_name": sensor_name,
            "start": start_iso,
            "end": end_iso,
        },
    )

    frame = _build_dataframe_from_measurements(payload)
    frame = _to_timeseries_columns(frame)
    return frame


def get_ground_temp(deveui, start, stop):
    close_all_stale_db_connections()
    start_iso, end_iso = _normalize_range_for_api(start, stop)

    sensor_names = _get_ground_temp_sensor_names(deveui)
    if not sensor_names:
        raise EmptyQuerySet("No data found for the given query")

    payload = _api_get(
        "api/v1/measurements",
        params={
            "deveui": deveui,
            "sensor_name__in": ",".join(sensor_names),
            "start": start_iso,
            "end": end_iso,
        },
    )

    frame = _build_dataframe_from_measurements(payload)

    # Keep the old dataframe contract where columns are ground-temperature sensor names.
    ground_columns = [
        col for col in frame.columns if "__" in col and "groundtemp" in col.lower()
    ]
    if not ground_columns:
        raise EmptyQuerySet("No data found for the given query")

    renamed = {col: col.split("__", 1)[0] for col in ground_columns}
    frame = frame[ground_columns].rename(columns=renamed)
    frame = frame.loc[:, ~frame.columns.duplicated()]

    # Legacy return contract expected a datatype object; unit is unused by current app code.
    datatype = None
    return frame, datatype


def get_ground_temp_sensor_depths(deveui):
    close_all_stale_db_connections()

    payload = _get_metadata_payload()
    depths = {}
    for sensor in payload.get("sensors", []):
        if sensor.get("recording_device_deveui") != deveui:
            continue
        name = sensor.get("name")
        if not name or "groundtemp" not in name.lower():
            continue
        depths[name] = sensor.get("relative_z")

    if not depths:
        raise EmptyQuerySet("No data found for the given query")

    # Keep deterministic ordering from shallow to deep matching the old behavior.
    return dict(sorted(depths.items(), key=lambda item: item[1], reverse=True))


# ---- async wrappers for the Dash callbacks ----------------------------------------------------
# thread_sensitive=False: each call runs in the executor's own threads (no ORM any more).

mast_catalogue_async = asgiref.sync.sync_to_async(mast_catalogue, thread_sensitive=False)
get_logger_data_async = asgiref.sync.sync_to_async(get_logger_data, thread_sensitive=False)

get_sensor_data_async = asgiref.sync.sync_to_async(get_sensor_data, thread_sensitive=False)
get_ground_temp_async = asgiref.sync.sync_to_async(get_ground_temp, thread_sensitive=False)
get_ground_temp_sensor_depths_async = asgiref.sync.sync_to_async(get_ground_temp_sensor_depths, thread_sensitive=False)
