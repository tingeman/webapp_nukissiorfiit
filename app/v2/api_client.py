"""Client of the monitoring_db read API for the phase 2 page (docs/plan/PLAN_api-v2.md, T8).

The best of webapp_grl_tsp/app/api_client.py (Client, ApiError, chunked /daily and /raw with
timing) and the phase 1 db_connector.py (root URL, connect-only retries, the mast catalogue,
the column selection), which this module does not import.

    client = Client.from_settings(config.settings)
    client.masts()                          -> the Nukissiorfiit masts and their loggers
    default_logger(mast)                    -> the logger with the latest data
    client.daily(logger, start, end)        -> DailyResult (series: key -> DailySeries)
    client.raw(logger, start, end)          -> RawResult (series: key -> RawSeries)
    battery(series), ground_temperature(series), inclination(series), heading(series),
    sensor_temperature(series), weather(series, name)  -> [Selected, ...]

``logger`` is a catalogue logger value (``deveui:<deveui>`` or ``serial:<station>:<serial>``).

Errors: every problem with the API (not reachable, timeout, HTTP error with the API's JSON
error shape, an answer that is not the expected JSON, the download time budget used up)
raises ``ApiError``: ``str(error)`` is a text of texts.py for the page, ``error.reason`` the
technical cause. The client has then logged one WARNING with the reason. A 504 (the API's
statement timeout) and a read timeout fail after one request; only connection errors are
retried (``BACKEND_API_MAX_RETRIES``), since such a request never reached the API.

Requests: GET <root>/api/v1/metadata (cached 10 minutes), GET <root>/api/v2/metadata?station=
(cached 10 minutes per station; failures are not cached), GET <root>/api/v2/measurements/daily
and .../raw with the logger's selector, ``start`` and ``end`` (no ``calibrated`` or
``include_masked``: the API's defaults apply). <root> is BACKEND_API_BASE_URL without a
trailing "/", "/api/v1" or "/api/v2" (one WARNING when a suffix is stripped).

Chunks: /daily is split at 1 August into requests of ``daily_chunk_years`` hydrological years
(``daily_chunks``); /raw into requests of ``raw_chunk_years`` years counted from the start
(``raw_chunks``). The API's start and end are both inclusive, so a chunk ends one second before
the next one starts. Dates cover whole UTC days; datetimes (UTC if naive) give exactly
start..end, for a zoom window. At most ``max_parallel`` requests are in flight; after an error
the queued chunks are dropped. With a ``deadline`` (``client.deadline_in(seconds)``) no chunk is
started after it: the call then fails with texts.DOWNLOAD_TOO_LARGE.

Data shape: per column (API key, in the order of first appearance) its details dict from the
API (sensor_name, timeseries_name, datatype, unit, relative_z, ...) and numpy arrays: raw
``time`` (datetime64[ms], UTC) and ``value`` (float); daily ``time`` (datetime64[D]) and
``mean``/``min``/``max`` (float) and ``count`` (int). Only points with data are kept.

Nothing but plain attributes and a lock is created in the constructor: gunicorn imports the app
with --preload and then forks, so sessions and thread pools are made per call.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import logging
import re
import threading
import time
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait

import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import texts

logger = logging.getLogger("nuki.v2.api")

METADATA_TTL_S = 600.0
HYDRO_YEAR_START_MONTH = 8          # 1 August, as webapp_grl_tsp
_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_API_SUFFIX = re.compile(r"/api/v[12]$")


class ApiError(Exception):
    """An API problem. str(e) is the message for the page (texts.py); e.reason the cause."""

    def __init__(self, message: str, reason: str):
        super().__init__(message)
        self.reason = reason


# ---- results --------------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class RequestTiming:
    start: str          # the request's start parameter
    end: str            # the request's end parameter
    seconds: float      # wall time of the request, including reading the body
    bytes: int          # size of the JSON body (after transfer decompression)


@dataclasses.dataclass(frozen=True)
class RawSeries:
    details: dict       # the API's column details (key, sensor_name, timeseries_name, unit, ...)
    time: np.ndarray    # datetime64[ms], UTC, ascending
    value: np.ndarray   # float


@dataclasses.dataclass(frozen=True)
class DailySeries:
    details: dict
    time: np.ndarray    # datetime64[D], UTC days, ascending, unique
    mean: np.ndarray    # float
    min: np.ndarray     # float
    max: np.ndarray     # float
    count: np.ndarray   # int, measurements per day (> 0)


@dataclasses.dataclass(frozen=True)
class DailyResult:
    payloads: list          # the API JSON of each chunk, in time order
    requests: list          # RequestTiming per chunk, in time order
    wall_seconds: float     # from the first request sent to the last answer read
    series: dict            # key -> DailySeries (RawSeries in RawResult), first appearance order

    @property
    def request_seconds(self) -> float:
        return sum(r.seconds for r in self.requests)

    @property
    def bytes(self) -> int:
        return sum(r.bytes for r in self.requests)


class RawResult(DailyResult):
    """The /raw JSON of each chunk, timing, and series: key -> RawSeries."""


# ---- root URL and logger values -------------------------------------------------------------

def root_url(configured: str) -> tuple[str, bool]:
    """(root, stripped): BACKEND_API_BASE_URL without a trailing "/" and "/api/v1" or "/api/v2"."""
    root = configured.rstrip("/")
    stripped = _API_SUFFIX.sub("", root)
    return stripped.rstrip("/"), stripped != root


def logger_params(logger_value: str) -> dict:
    """Query parameters that select a catalogue logger (``deveui:...`` or ``serial:<station>:<serial>``;
    the API cannot combine a deveui with a station). ValueError for another value."""
    kind, _, rest = (logger_value or "").partition(":")
    if kind == "deveui" and rest:
        return {"deveui": rest}
    if kind == "serial":
        station, _, serial = rest.partition(":")
        if station and serial:
            return {"station": station, "serial": serial}
    raise ValueError(f"Invalid logger: {logger_value!r}")


# ---- chunks ---------------------------------------------------------------------------------

def _as_utc(t: dt.datetime) -> dt.datetime:
    return t.replace(tzinfo=dt.timezone.utc) if t.tzinfo is None else t.astimezone(dt.timezone.utc)


def _coerce(value):
    """A date or datetime; strings: "YYYY-MM-DD" is a date, other ISO 8601 a datetime (Z allowed)."""
    if isinstance(value, str):
        if len(value) == 10:
            return dt.date.fromisoformat(value)
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def _bounds(start, end, now):
    """(first, last) UTC datetimes, whole seconds: dates cover whole days, datetimes are exact;
    last is at most now. Strings are read by _coerce."""
    start, end = _coerce(start), _coerce(end)
    now = _as_utc(now or dt.datetime.now(dt.timezone.utc)).replace(microsecond=0)
    if isinstance(start, dt.datetime):
        first = _as_utc(start).replace(microsecond=0)
    else:
        first = dt.datetime(start.year, start.month, start.day, tzinfo=dt.timezone.utc)
    if isinstance(end, dt.datetime):
        last = _as_utc(end).replace(microsecond=0)
    else:
        last = dt.datetime(end.year, end.month, end.day, tzinfo=dt.timezone.utc) + dt.timedelta(days=1, seconds=-1)
    return first, min(last, now)


def _pieces(first, last, next_boundary):
    out, lo = [], first
    while lo <= last:
        boundary = next_boundary(lo)
        hi = min(boundary - dt.timedelta(seconds=1), last)
        out.append((lo.strftime(_TS_FORMAT), hi.strftime(_TS_FORMAT)))
        lo = boundary
    return out


def daily_chunks(start, end, years: int, now: dt.datetime | None = None) -> list:
    """(start, end) request parameters covering start..end, split at 1 August into pieces of
    ``years`` hydrological years (webapp_grl_tsp's rule). Dates: the days start..end (both
    inclusive); datetimes (UTC if naive): exactly start..end. Consecutive pieces touch without
    overlap (end = next start - 1 s); the last ends at ``end`` or ``now``, whichever is earlier.
    Empty if the period lies after ``now`` or end is before start."""
    if years < 1:
        raise ValueError("years must be 1 or more")
    first, last = _bounds(start, end, now)

    def next_boundary(lo):
        hydro_year = lo.year if lo.month >= HYDRO_YEAR_START_MONTH else lo.year - 1
        return dt.datetime(hydro_year + years, HYDRO_YEAR_START_MONTH, 1, tzinfo=dt.timezone.utc)
    return _pieces(first, last, next_boundary)


def _add_years(when: dt.datetime, years: int) -> dt.datetime:
    try:
        return when.replace(year=when.year + years)
    except ValueError:      # 29 February -> 1 March
        return when.replace(year=when.year + years, month=3, day=1)


def raw_chunks(start, end, years: int, now: dt.datetime | None = None) -> list:
    """As ``daily_chunks``, but in pieces of ``years`` years counted from the start (phase 1's
    rule: a 366-day Full view is 1 or 2 requests, wherever it starts)."""
    if years < 1:
        raise ValueError("years must be 1 or more")
    first, last = _bounds(start, end, now)
    return _pieces(first, last, lambda lo: _add_years(lo, years))


# ---- answers to series ----------------------------------------------------------------------

def _times_ms(strings) -> np.ndarray:
    return np.array([s[:-1] if s.endswith("Z") else s for s in strings], dtype="datetime64[ms]")


def _floats(values) -> np.ndarray:
    return np.array(values, dtype=float)      # None -> NaN


def _raw_series(payloads) -> dict:
    parts: dict = {}
    details: dict = {}
    for p in payloads:
        t = _times_ms(p["time"])
        for c, vals in zip(p["columns"], p["values"]):
            v = _floats(vals)
            ok = ~np.isnan(v)
            details.setdefault(c["key"], dict(c))
            parts.setdefault(c["key"], []).append((t[ok], v[ok]))
    out = {}
    for key, chunks_ in parts.items():
        t = np.concatenate([a for a, _ in chunks_])
        v = np.concatenate([b for _, b in chunks_])
        if len(t) > 1 and (np.diff(t) < np.timedelta64(0, "ms")).any():
            order = np.argsort(t, kind="stable")
            t, v = t[order], v[order]
        out[key] = RawSeries(details=details[key], time=t, value=v)
    return out


def _daily_series(payloads) -> dict:
    parts: dict = {}
    details: dict = {}
    for p in payloads:
        days = np.array(p["time"], dtype="datetime64[D]")
        vals = p["values"]
        for i, c in enumerate(p["columns"]):
            count = np.nan_to_num(_floats(vals["count"][i])).astype(np.int64)
            mean = _floats(vals["mean"][i])
            ok = (count > 0) & ~np.isnan(mean)
            lo, hi = _floats(vals["min"][i]), _floats(vals["max"][i])
            lo, hi = np.where(np.isnan(lo), mean, lo), np.where(np.isnan(hi), mean, hi)
            details.setdefault(c["key"], dict(c))
            parts.setdefault(c["key"], []).append((days[ok], mean[ok], lo[ok], hi[ok], count[ok]))
    out = {}
    for key, chunks_ in parts.items():
        d, mean, lo, hi, count = (np.concatenate(a) for a in zip(*chunks_))
        days, inverse = np.unique(d, return_inverse=True)
        if len(days) != len(d):     # a day in two chunks: merge, mean weighted by count
            n = np.zeros(len(days), dtype=np.int64)
            wsum = np.zeros(len(days))
            vmin, vmax = np.full(len(days), np.inf), np.full(len(days), -np.inf)
            np.add.at(n, inverse, count)
            np.add.at(wsum, inverse, mean * count)
            np.minimum.at(vmin, inverse, lo)
            np.maximum.at(vmax, inverse, hi)
            mean, lo, hi, count = wsum / n, vmin, vmax, n
        else:
            order = np.argsort(d, kind="stable")
            mean, lo, hi, count = mean[order], lo[order], hi[order], count[order]
        out[key] = DailySeries(details=details[key], time=days, mean=mean, min=lo, max=hi, count=count)
    return out


# ---- client ---------------------------------------------------------------------------------

class Client:
    def __init__(self, base_url: str, timeout_s: float = 30.0, max_retries: int = 2,
                 backoff_factor: float = 0.5, max_parallel: int = 2, daily_chunk_years: int = 3,
                 raw_chunk_years: int = 1, station_pattern: str = r"^(SIS2024-0[1-5]|NUKI202[0-9]-.+)$",
                 clock=time.monotonic):
        self.base_url, stripped = root_url(base_url)
        if stripped:
            logger.warning("BACKEND_API_BASE_URL %s ends with an API version; using the root %s "
                           "(set the root form to silence this warning)", base_url, self.base_url)
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.max_parallel = max_parallel
        self.daily_chunk_years = daily_chunk_years
        self.raw_chunk_years = raw_chunk_years
        self.station_regex = re.compile(station_pattern)
        self._clock = clock
        self._lock = threading.Lock()
        self._v1_metadata = None        # (clock time, JSON)
        self._v2_metadata = {}          # station identifier -> (clock time, JSON)

    @classmethod
    def from_settings(cls, settings) -> "Client":
        return cls(settings.backend_api_base_url, settings.backend_api_timeout_seconds,
                   settings.backend_api_max_retries, settings.backend_api_backoff_factor,
                   settings.backend_api_max_parallel, settings.backend_api_daily_chunk_years,
                   settings.backend_api_raw_chunk_years, settings.nuki_station_pattern)

    def clear_cache(self):
        with self._lock:
            self._v1_metadata = None
            self._v2_metadata.clear()

    def deadline_in(self, seconds: float) -> float:
        """A deadline for daily()/raw(), ``seconds`` from now (on the client's clock)."""
        return self._clock() + seconds

    # ---- transport ----

    def _session(self) -> requests.Session:
        # R1: connection errors are retried (the request never reached the API); a read
        # timeout (read=False: re-raised as requests' ReadTimeout) and any HTTP status (no
        # status_forcelist) are not, so a 504 or a slow query costs the API one request.
        retry = Retry(total=self.max_retries, connect=self.max_retries, read=False, status=0, other=0,
                      backoff_factor=self.backoff_factor, allowed_methods=["GET"],
                      respect_retry_after_header=False, raise_on_status=False)
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get(self, path: str, params: dict | None = None):
        """(JSON object, seconds, bytes) of one GET <root><path>; raises ApiError (not logged here)."""
        url = self.base_url + path
        t0 = time.perf_counter()
        try:
            with self._session() as session:
                r = session.get(url, params=params, timeout=self.timeout_s)
                body = r.content
        except requests.Timeout:
            raise ApiError(texts.API_TIMEOUT, f"timeout after {self.timeout_s:g} s") from None
        except requests.RequestException as e:
            raise ApiError(texts.API_UNREACHABLE, f"{type(e).__name__}: {e}") from None
        seconds = time.perf_counter() - t0
        if not 200 <= r.status_code < 300:
            code, message = None, None
            try:
                err = r.json().get("error") or {}
                code, message = err.get("code"), err.get("message")
            except (ValueError, AttributeError):
                pass
            reason = f"HTTP {r.status_code} {code or ''} {message or body[:200]!r}".strip()
            if r.status_code == 504:
                raise ApiError(texts.API_TIMEOUT, reason)
            detail = f"{message} ({code}, HTTP {r.status_code})" if message else f"HTTP {r.status_code}"
            raise ApiError(texts.API_ERROR.format(detail=detail), reason)
        try:
            data = r.json()
        except ValueError:
            raise ApiError(texts.API_INVALID, f"invalid JSON ({len(body)} bytes)") from None
        if not isinstance(data, dict):
            raise ApiError(texts.API_INVALID, f"JSON {type(data).__name__}, not an object")
        return data, seconds, len(body)

    # ---- metadata ----

    def _cached(self, what: str, get_hit, put, path: str, params: dict | None):
        with self._lock:
            hit = get_hit()
            if hit is not None and self._clock() - hit[0] < METADATA_TTL_S:
                return hit[1]
        try:
            data, seconds, size = self._get(path, params)      # outside the lock
        except ApiError as e:
            logger.warning("API %s failed: %s (%s%s %s)", what, e.reason, self.base_url, path, params or "")
            raise
        logger.info("API %s: %.2f s, %d bytes", what, seconds, size)
        with self._lock:
            put((self._clock(), data))
        return data

    def metadata_v1(self) -> dict:
        """GET /api/v1/metadata (whole database), cached for 10 minutes (failures are not)."""
        return self._cached("metadata", lambda: self._v1_metadata,
                            lambda v: setattr(self, "_v1_metadata", v), "/api/v1/metadata", None)

    def metadata_v2(self, station: str) -> dict:
        """GET /api/v2/metadata?station=<identifier>, cached per station for 10 minutes."""
        return self._cached("v2 metadata", lambda: self._v2_metadata.get(station),
                            lambda v: self._v2_metadata.__setitem__(station, v),
                            "/api/v2/metadata", {"station": station})

    def masts(self) -> list:
        """The Nukissiorfiit masts with their loggers, sorted by mast number:

            [{"name": "Mast 6", "station_identifiers": ["SIS2024-01"],
              "loggers": [{"value": "deveui:..." | "serial:<station>:<serial>",
                           "label": "<display name> (<serial or deveui>), <first> to <last>",
                           "station": "<identifier>",
                           "first": "YYYY-MM-DDTHH:MM:SSZ", "last": "YYYY-MM-DDTHH:MM:SSZ"}, ...]}]

        Stations: identifier fully matches NUKI_STATION_PATTERN, grouped by station name.
        Loggers (by first measurement) come from the v2 metadata of each station; loggers
        without measurements, and masts without such a logger, are left out. Raises ApiError."""
        meta = self.metadata_v1()
        try:
            groups: dict = {}
            for station in meta.get("stations") or []:
                ident = station.get("identifier")
                if not ident or not self.station_regex.fullmatch(ident):
                    continue
                groups.setdefault(station.get("name") or ident, []).append(ident)
            masts = []
            for name, idents in groups.items():
                loggers = []
                for ident in idents:
                    for device in self.metadata_v2(ident).get("recording_devices") or []:
                        if not device.get("last_measurement_time"):
                            continue
                        entry = _logger_entry(ident, device)
                        if entry is not None:
                            loggers.append(entry)
                if not loggers:
                    continue
                loggers.sort(key=lambda lg: (lg["first"] or "", lg["value"]))
                masts.append({"name": _mast_label(name), "station_identifiers": idents, "loggers": loggers})
        except (KeyError, TypeError, ValueError, AttributeError) as e:
            logger.warning("API metadata has an unexpected shape: %s: %s", type(e).__name__, e)
            raise ApiError(texts.API_INVALID, f"metadata shape: {e}") from None
        masts.sort(key=lambda m: _mast_sort_key(m["name"]))
        return masts

    # ---- measurements ----

    def _chunk(self, kind: str, params: dict, start: str, end: str, deadline):
        if deadline is not None and self._clock() >= deadline:
            raise ApiError(texts.DOWNLOAD_TOO_LARGE, f"time budget used up before the {kind} chunk {start}..{end}")
        data, seconds, size = self._get(f"/api/v2/measurements/{kind}", {**params, "start": start, "end": end})
        _check_shape(kind, data)
        return data, RequestTiming(start, end, seconds, size)

    def _fetch(self, kind: str, params: dict, pieces: list, deadline=None):
        """(payloads, timings, wall seconds) of the chunk requests, at most max_parallel at
        once; no chunk is started after ``deadline`` (client clock). Raises ApiError (one
        WARNING logged); after an error the queued chunks are dropped."""
        t0 = time.perf_counter()
        results: list = [None] * len(pieces)
        try:
            if len(pieces) == 1:
                results[0] = self._chunk(kind, params, *pieces[0], deadline)
            elif pieces:
                pool = ThreadPoolExecutor(max_workers=min(self.max_parallel, len(pieces)),
                                          thread_name_prefix="mdb-api")
                try:
                    futures = {pool.submit(self._chunk, kind, params, s, e, deadline): i
                               for i, (s, e) in enumerate(pieces)}
                    done, _ = wait(futures, return_when=FIRST_EXCEPTION)
                    for f in done:
                        if f.exception() is not None:
                            raise f.exception()
                    for f, i in futures.items():
                        results[i] = f.result()
                finally:
                    # after an error: drop the queued chunks, do not wait for running ones
                    pool.shutdown(wait=False, cancel_futures=True)
        except ApiError as e:
            span = f"{pieces[0][0]}..{pieces[-1][1]}" if pieces else "-"
            logger.warning("API %s failed: %s (%s, %s, %d request(s))",
                           kind, e.reason, _describe(params), span, len(pieces))
            raise
        wall = time.perf_counter() - t0
        return [r[0] for r in results], [r[1] for r in results], wall

    def daily(self, logger_value: str, start, end, now: dt.datetime | None = None,
              deadline: float | None = None) -> DailyResult:
        """The daily aggregates of start..end (dates: whole days, inclusive; datetimes: exact)
        of one catalogue logger, in daily_chunk_years hydrological-year chunks. Raises ApiError
        (ValueError for an invalid logger value)."""
        params = logger_params(logger_value)
        pieces = daily_chunks(start, end, self.daily_chunk_years, now)
        payloads, timings, wall = self._fetch("daily", params, pieces, deadline)
        return DailyResult(payloads, timings, wall, _daily_series(payloads))

    def raw(self, logger_value: str, start, end, now: dt.datetime | None = None,
            deadline: float | None = None) -> RawResult:
        """Every measurement of start..end of one catalogue logger, in raw_chunk_years chunks
        from the start. Dates cover whole days (inclusive); datetimes (UTC if naive) give
        exactly start..end, so a zoom window of minutes is one request. Raises ApiError."""
        params = logger_params(logger_value)
        pieces = raw_chunks(start, end, self.raw_chunk_years, now)
        payloads, timings, wall = self._fetch("raw", params, pieces, deadline)
        return RawResult(payloads, timings, wall, _raw_series(payloads))


def _aligned(arrays, columns, times) -> bool:
    return (isinstance(arrays, list) and len(arrays) == len(columns)
            and all(isinstance(v, list) and len(v) == len(times) for v in arrays))


def _check_shape(kind: str, data: dict):
    times, columns, values = data.get("time"), data.get("columns"), data.get("values")
    if not isinstance(times, list) or not isinstance(columns, list) \
            or not all(isinstance(c, dict) and isinstance(c.get("key"), str) and c["key"] for c in columns):
        raise ApiError(texts.API_INVALID, f"{kind} answer without time list and keyed columns")
    if kind == "raw":
        ok = _aligned(values, columns, times)
    else:
        ok = isinstance(values, dict) and all(_aligned(values.get(k), columns, times)
                                              for k in ("mean", "min", "max", "count"))
    if not ok:
        raise ApiError(texts.API_INVALID, f"{kind} values are not one array per column aligned with time")


def _describe(params: dict) -> str:
    return " ".join(f"{k}={v}" for k, v in params.items())


# ---- mast catalogue helpers (from db_connector) ---------------------------------------------

_MAST_PREFIX = "Nukissiorfiit "


def _mast_label(name: str) -> str:
    label = name[len(_MAST_PREFIX):] if name.startswith(_MAST_PREFIX) else name
    return label.strip() or name


def _mast_sort_key(label: str):
    m = re.search(r"\d+", label)
    return (0, int(m.group()), label) if m else (1, 0, label)


def _logger_entry(station: str, device: dict):
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


def default_logger(mast: dict):
    """The logger of a catalogue mast with the latest last measurement (None if it has none)."""
    loggers = mast.get("loggers") or []
    if not loggers:
        return None
    return max(loggers, key=lambda lg: lg["last"])      # YYYY-MM-DDTHH:MM:SSZ sorts as time


# ---- column selection (no HTTP) -------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class Selected:
    key: str            # the API column key
    label: str          # legend label
    unit: str | None    # the unit of the (converted) values
    series: object      # RawSeries or DailySeries


def _where(series: dict, test) -> list:
    return [(k, s) for k, s in series.items() if test(s.details)]


def battery(series: dict) -> list:
    """The BatteryVoltage series, labelled by series name."""
    return [Selected(k, s.details["timeseries_name"], s.details.get("unit"), s)
            for k, s in _where(series, lambda c: c.get("timeseries_name") == "BatteryVoltage")]


def ground_temperature(series: dict) -> list:
    """The GroundTemperature series with relative_z <= 0, shallow to deep, labelled "-0.10 m"."""
    found = _where(series, lambda c: c.get("timeseries_name") == "GroundTemperature"
                   and c.get("relative_z") is not None and float(c["relative_z"]) <= 0)
    found.sort(key=lambda ks: -float(ks[1].details["relative_z"]))
    return [Selected(k, f"{float(s.details['relative_z']):+0.2f} m", s.details.get("unit"), s) for k, s in found]


def inclination(series: dict) -> list:
    """The series with datatype Angle (Tekbox Roll/Pitch, Campbell X/Y), labelled by series name."""
    return [Selected(k, s.details["timeseries_name"], s.details.get("unit"), s)
            for k, s in _where(series, lambda c: c.get("datatype") == "Angle")]


def heading(series: dict) -> list:
    """The series with datatype Heading (Tekbox CompassHeading), labelled by series name."""
    return [Selected(k, s.details["timeseries_name"], s.details.get("unit"), s)
            for k, s in _where(series, lambda c: c.get("datatype") == "Heading")]


def sensor_temperature(series: dict) -> list:
    """The SensorTemp series (inclinometers), labelled by sensor name."""
    return [Selected(k, s.details.get("sensor_name") or k, s.details.get("unit"), s)
            for k, s in _where(series, lambda c: c.get("timeseries_name") == "SensorTemp")]


WEATHER_SERIES = ("AirTemp", "RelHum", "BarometricPressure")
_TO_KPA = {"Pa": 0.001, "hPa": 0.1, "kPa": 1.0}


def _scaled(s, factor):
    if isinstance(s, RawSeries):
        return dataclasses.replace(s, value=s.value * factor)
    return dataclasses.replace(s, mean=s.mean * factor, min=s.min * factor, max=s.max * factor)


def weather(series: dict, name: str) -> list:
    """One weather series ("AirTemp", "RelHum" or "BarometricPressure"), labelled by name.
    Pressure is converted to kPa by the column's unit (Pa, hPa, kPa); an unknown unit is
    returned unconverted with its own unit, and a WARNING is logged."""
    if name not in WEATHER_SERIES:
        raise ValueError(f"Unknown weather series: {name!r}")
    out = []
    for k, s in _where(series, lambda c: c.get("timeseries_name") == name):
        unit = s.details.get("unit")
        if name == "BarometricPressure":
            factor = _TO_KPA.get(unit)
            if factor is None:
                logger.warning("BarometricPressure column %s has unit %r, not Pa/hPa/kPa: not converted", k, unit)
            else:
                s, unit = _scaled(s, factor), "kPa"
        out.append(Selected(k, name, unit, s))
    return out
