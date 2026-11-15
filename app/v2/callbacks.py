"""Callbacks of the phase 2 page (docs/plan/PLAN_api-v2.md, T9).

``register(app, client)`` attaches them with ``app.callback`` to the phase 2 Dash instance
only. Do not use the global ``dash.callback`` decorator here (or in app.py): with two Dash
apps on one server, Dash copies the global callbacks into the first app that sets itself up
(on the first request to the server, whatever its path), which is not necessarily the page
they belong to. Callbacks are plain sync functions (gunicorn runs sync workers); ``client``
is the process's one api_client.Client (its metadata caches live on it).

Data flow (requests per action in the plan, "Phase 2 design"):

1. Page load (``url``): the catalogue (``client.masts()``: v1 metadata and v2 metadata per
   station, cached by the client 10 minutes) into the ``masts`` store; the first mast.
2. Mast -> logger options, default the logger with the latest data (``default_logger``).
3. Logger -> date picker limits and range: the logger's first..last measurement day.
4. Logger, period or Daily/Full (``resolution``) -> ``load``: Daily is one /daily request
   set for the period (mean lines with min/max bands). Full is /raw for the period, at most
   one year (a longer period loads the year up to its end date, 365 or 366 days, and the info
   line says so; with BACKEND_API_RAW_CHUNK_YEARS=1 that is always one /raw request), thinned to about figures.MAX_POINTS points per series. The ``view`` store keeps what
   the figure shows: {"logger", "mode", "range": [lo, hi] ISO datetimes, "keys": the API
   column key of each trace (None for band traces; All loggers: the join key), "note",
   All loggers also "loggers": the mast's catalogue loggers}.
5. Zoom (``data-graph.relayoutData``) in Full: /raw for the visible window only (one request
   set, exactly the window's datetimes), thinned and patched into the traces by key (the
   figure's zoom stays as the user left it); autorange (reset) loads the stored range again.
   Daily makes no request on a zoom. Nothing is kept on the server between requests.
6. The ground temperature profile (T10): ``load`` also fills the ``profile`` store
   (profile.py: the daily ground temperatures, compact arrays). Daily reuses the load's
   /daily result; Full makes one extra /daily request set for the same (one-year) range,
   after the raw one. A zoom makes none. The store sets the day slider (limits, marks,
   default the last day) or hides the profile with a note; the slider, the envelope choice
   or a new store draw the profile from the store alone (no API request) and mark the day
   on the data figure (a Patch of its shapes).

7. All loggers (T11): the logger dropdown's last option (``all:<mast name>``, only for a mast
   with 2 or more loggers). The date limits and default cover the union of the loggers'
   periods. The loggers whose period overlaps the range are fetched one after the other
   (never two loggers' requests in flight; an ApiError stops, the loggers after it are not
   fetched, and nothing half-joined is shown), each for its part of the range only, and
   joined by join.join (per station: ground temperature by depth, the rest by series name),
   with a dashed line at each logger change. Daily: one /daily request set per logger. Full:
   /raw for the one-year window (full_period) per logger with data in it, then the profile's
   /daily per such logger. The ``view`` store then also holds the mast's ``loggers``, and a
   zoom refetches /raw for the window per logger with data in it. The ``profile`` store comes
   from the joined ground temperatures (profile.for_day picks one station per day).

Problems end up as an alert on the page (``str(ApiError)``, texts.py) and one log line,
never as a traceback on the page.
"""
from __future__ import annotations

import datetime as dt
import logging
import re
import time
import uuid

import dash_bootstrap_components as dbc
from dash import Input, Output, Patch, State, html, no_update

from . import api_client, figures, join, profile, texts

logger = logging.getLogger("nuki.v2")

UTC = dt.timezone.utc


# ---- helpers (plain functions, no Dash state) -------------------------------------------------

def _alert(text: str, color: str):
    return dbc.Alert(text, color=color, className="nuki-alert mb-3", dismissable=False)


def size_text(n: int) -> str:
    if n < 1024 * 1024:
        return f"{max(n, 0) / 1024:.0f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


def bin_text(seconds: float) -> str:
    """A bin width as people say it: '30 s', '15 min', '6 h', '2 d' (as TSP)."""
    if round(seconds) < 60:
        return f"{max(round(seconds), 1)} s"
    if round(seconds / 60) < 60:
        return f"{round(seconds / 60)} min"
    if round(seconds / 3600) < 48:
        return f"{round(seconds / 3600)} h"
    return f"{round(seconds / 86400)} d"


def _date(value) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _utc_now() -> dt.datetime:
    return dt.datetime.now(UTC).replace(microsecond=0)


def find_mast(masts, name):
    return next((m for m in masts or [] if m["name"] == name), None)


ALL_PREFIX = "all:"


def all_value(mast_name: str) -> str:
    """The logger dropdown value of "All loggers" of a mast."""
    return ALL_PREFIX + mast_name


def is_all(value) -> bool:
    return isinstance(value, str) and value.startswith(ALL_PREFIX)


def logger_options(mast) -> list:
    """The logger dropdown options of a catalogue mast: its loggers, and "All loggers" last
    when it has 2 or more."""
    options = [{"label": lg["label"], "value": lg["value"]} for lg in mast["loggers"]]
    if len(mast["loggers"]) >= 2:
        options.append({"label": texts.LOGGER_ALL, "value": all_value(mast["name"])})
    return options


def logger_period(masts, value):
    """(first, last) "YYYY-MM-DD" of a logger value (All loggers: the union of the mast's
    loggers' periods), or None."""
    if is_all(value):
        mast = find_mast(masts, value[len(ALL_PREFIX):])
        loggers = [lg for lg in (mast or {}).get("loggers", []) if lg.get("last")]
        if not loggers:
            return None
        return (min((lg.get("first") or lg["last"])[:10] for lg in loggers),
                max(lg["last"][:10] for lg in loggers))
    lg = find_logger(masts, value)
    if lg is None or not lg.get("last"):
        return None
    return (lg.get("first") or lg["last"])[:10], lg["last"][:10]


def overlapping(loggers, start: dt.date, end: dt.date) -> list:
    """[(logger, first, last)] of the loggers whose period overlaps the days start..end, each
    with the overlap (dates), in the given (catalogue) order."""
    out = []
    for lg in loggers or []:
        first, last = _date(lg.get("first") or lg.get("last")), _date(lg.get("last"))
        if first is None or last is None:
            continue
        a, b = max(start, first), min(end, last)
        if a <= b:
            out.append((lg, a, b))
    return out


def fetch_each(fetch, items) -> list:
    """[(logger, result)] of ``fetch(logger value, start, end)`` for each (logger, start, end),
    one after the other; an ApiError propagates at once (the loggers after it are not
    fetched)."""
    out = []
    for lg, a, b in items:
        out.append((lg, fetch(lg["value"], a, b)))
    return out


def find_logger(masts, value):
    for m in masts or []:
        for lg in m["loggers"]:
            if lg["value"] == value:
                return lg
    return None


def full_period(start: dt.date, end: dt.date):
    """(start, end, limited) of a Full load: the period, or the year up to its end date
    (365 or 366 days: the day after ``end``, one year back), so it is one raw chunk."""
    after = end + dt.timedelta(days=1)
    try:
        lo = after.replace(year=after.year - 1)
    except ValueError:      # 29 February -> 1 March
        lo = after.replace(year=after.year - 1, month=3, day=1)
    if start < lo:
        return lo, end, True
    return start, end, False


def day_bounds(start: dt.date, end: dt.date):
    """(first, last) UTC datetimes of the days start..end (whole seconds), last at most now."""
    lo = dt.datetime(start.year, start.month, start.day, tzinfo=UTC)
    hi = dt.datetime(end.year, end.month, end.day, tzinfo=UTC) + dt.timedelta(days=1, seconds=-1)
    return lo, min(hi, _utc_now())


def parse_time(value) -> dt.datetime | None:
    """A Plotly axis value ('2021-08-03 12:34:56.789'; ISO with T or Z also) as UTC."""
    if not isinstance(value, str):
        return None
    text = value.strip().replace(" ", "T").removesuffix("Z")
    text, _, frac = text.partition(".")
    try:
        t = dt.datetime.fromisoformat(text + ("." + frac[:6].ljust(6, "0") if frac else ""))
    except ValueError:
        return None
    return t.replace(tzinfo=UTC) if t.tzinfo is None else t.astimezone(UTC)


_X_RANGE0 = re.compile(r"^(xaxis\d*)\.range\[0\]$")
_X_RANGE = re.compile(r"^(xaxis\d*)\.range$")
_X_AUTO = re.compile(r"^xaxis\d*\.autorange$")


def zoom_target(relayout):
    """What a relayoutData of the figure asks for: "overview" (x autorange of any row), (lo,
    hi) UTC datetimes of a new x range (of any row: the x axes are matched), or None
    (anything else: autosize, a y axis only, ...)."""
    if not isinstance(relayout, dict):
        return None
    if any(_X_AUTO.match(k) and v is True for k, v in relayout.items()):
        return "overview"
    pair = None
    for k, v in relayout.items():
        m = _X_RANGE0.match(k)
        if m and f"{m.group(1)}.range[1]" in relayout:
            pair = [v, relayout[f"{m.group(1)}.range[1]"]]
            break
        if _X_RANGE.match(k):
            pair = v
            break
    if not isinstance(pair, (list, tuple)) or len(pair) != 2:
        return None
    lo, hi = parse_time(pair[0]), parse_time(pair[1])
    if lo is None or hi is None:
        return None
    return (lo, hi) if lo <= hi else (hi, lo)


def clip_window(window, bounds):
    """The window cut to bounds (lo, hi), whole seconds, or None if nothing is left."""
    lo, hi = max(window[0], bounds[0]), min(window[1], bounds[1])
    lo, hi = lo.replace(microsecond=0), hi.replace(microsecond=0)
    return (lo, hi) if lo < hi else None


def _fmt_day(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%d")


def _fmt_time(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M")


def info_items(what: str, result, note: str | None = None, profile_result=None):
    """The info line: what the figure shows, the API part (requests, wall seconds, size) and
    the requests one by one (folded). ``profile_result``: the extra /daily of a Full load
    (made after the raw one: the seconds add up); its requests are listed after the raw
    ones, each line with its kind."""
    if profile_result is None:
        return info_parts(what, [(None, result)], note)
    return info_parts(what, [(texts.INFO_KIND_RAW, result), (texts.INFO_KIND_PROFILE, profile_result)], note)


def info_parts(what: str, parts, note: str | None = None, loggers=None):
    """The info line of several results made one after the other: ``parts`` [(prefix or
    None, result)], each request line starting with its part's prefix (a kind, or for All
    loggers "<logger>, <kind>"); ``loggers``: the names of the loggers fetched (All loggers;
    an empty list says no logger has data in the period)."""
    requests = [(prefix, r) for prefix, res in parts for r in res.requests]
    api = texts.INFO_REQUESTS.format(requests=len(requests),
                                     seconds=sum(res.wall_seconds for _, res in parts),
                                     size=size_text(sum(res.bytes for _, res in parts)))
    lines = [html.Span(what, className="nuki-info-what"), html.Span(" · " + api, className="nuki-info-api")]
    out = [html.Div(lines)]
    if loggers is not None:
        out.append(html.Div(texts.INFO_LOGGERS.format(loggers="; ".join(loggers) or texts.INFO_NO_LOGGER),
                            className="nuki-info-loggers"))
    if note:
        out.append(html.Div(note, className="nuki-info-note"))
    out.append(html.Details([
        html.Summary(texts.INFO_DETAILS),
        html.Ul([html.Li((texts.INFO_REQUEST_KIND.format(kind=prefix) if prefix else "")
                         + texts.INFO_REQUEST_LINE.format(start=r.start, end=r.end, seconds=r.seconds,
                                                          size=size_text(r.bytes)))
                 for prefix, r in requests], className="nuki-requests"),
    ], className="nuki-details"))
    return out


def _prefixed(parts, kind=None) -> list:
    """[(prefix, result)] of All loggers' [(logger, result)]: "<logger>" or "<logger>, <kind>"."""
    return [(join.short_label(lg) + (f", {kind}" if kind else ""), res) for lg, res in parts]


def _full_what(lo, hi, bin_seconds, zoomed=False):
    if zoomed:
        what = texts.INFO_ZOOM.format(start=_fmt_time(lo), end=_fmt_time(hi))
        if bin_seconds is not None:
            what += " · " + texts.INFO_THINNED.format(start=_fmt_time(lo), end=_fmt_time(hi),
                                                       bin=bin_text(bin_seconds))
        return what
    if bin_seconds is None:
        return texts.INFO_FULL.format(start=_fmt_day(lo), end=_fmt_day(hi))
    return texts.INFO_THINNED.format(start=_fmt_day(lo), end=_fmt_day(hi), bin=bin_text(bin_seconds))


def load(client: api_client.Client, logger_value, start, end, resolution, masts=None):
    """Everything a logger/period/resolution change produces, as plain values: figure,
    info (children), message (alert or None), view (store or None), profile (store or
    None, profile.py). Raises ApiError (not for the profile request of a Full load: its
    error goes into the profile store, and the Full figure is shown). ``masts``: the
    catalogue (the ``masts`` store), needed for All loggers only."""
    out = {"figure": figures.empty_figure(texts.FIGURE_EMPTY), "info": None, "message": None,
           "view": None, "profile": None}
    if not logger_value:
        return out
    start, end = _date(start), _date(end)
    if start is None or end is None:
        out["message"] = _alert(texts.CHOOSE_PERIOD, "warning")
        return out
    if start > end:
        out["message"] = _alert(texts.PERIOD_REVERSED, "warning")
        return out
    if is_all(logger_value):
        mast = find_mast(masts, logger_value[len(ALL_PREFIX):])
        if mast is None or len(mast.get("loggers") or []) < 2:
            out["message"] = _alert(texts.CHOOSE_FIRST, "warning")
            return out
        return load_all(client, mast, logger_value, start, end, resolution, out)
    try:
        api_client.logger_params(logger_value)
    except ValueError:
        out["message"] = _alert(texts.CHOOSE_FIRST, "warning")
        return out
    revision = uuid.uuid4().hex[:8]
    t0 = time.perf_counter()
    if resolution == "full":
        start, end, limited = full_period(start, end)
        lo, hi = day_bounds(start, end)
        result = client.raw(logger_value, lo, hi)
        rows = figures.rows(result.series)
        fig, keys, bin_s = figures.data_figure(rows, "raw", revision, window=(lo, hi))
        note = texts.INFO_FULL_LIMIT if limited else None
        what = _full_what(lo, hi, bin_s)
        mode = "full"
        profile_result = None
        try:        # the profile: one /daily request set for the same days
            profile_result = client.daily(logger_value, start, end)
            out["profile"] = profile.store_data(api_client.ground_temperature(profile_result.series),
                                                logger_value)
        except api_client.ApiError as e:        # the client logged it
            out["profile"] = profile.empty(logger_value, str(e))
    else:
        lo, hi = day_bounds(start, end)
        result = client.daily(logger_value, start, end)
        rows = figures.rows(result.series)
        fig, keys, _ = figures.data_figure(rows, "daily", revision)
        note = None
        what = texts.INFO_DAILY.format(start=start.isoformat(), end=end.isoformat())
        mode = "daily"
        profile_result = None       # the profile reuses this /daily result
        out["profile"] = profile.store_data(api_client.ground_temperature(result.series), logger_value)
    logger.info("v2 load %s %s %s..%s: %d request(s), API %.2f s, %d bytes, %d rows, %d traces, %.2f s",
                logger_value, mode, lo.isoformat(), hi.isoformat(), len(result.requests),
                result.wall_seconds, result.bytes, len(rows), len(keys), time.perf_counter() - t0)
    if not rows:
        out["message"] = _alert(texts.NO_DATA.format(start=start.isoformat(), end=end.isoformat()), "info")
        out["info"] = info_items(what, result, note, profile_result)
        return out
    out["figure"] = fig
    out["info"] = info_items(what, result, note, profile_result)
    out["view"] = {"logger": logger_value, "mode": mode, "range": [lo.isoformat(), hi.isoformat()],
                   "keys": keys, "note": note}
    return out


def load_all(client: api_client.Client, mast, logger_value, start: dt.date, end: dt.date,
             resolution, out):
    """``load`` for All loggers of ``mast`` (see the module docstring, 7.)."""
    revision = uuid.uuid4().hex[:8]
    t0 = time.perf_counter()
    loggers = mast["loggers"]
    note = None
    if resolution == "full":
        start, end, limited = full_period(start, end)
        note = texts.INFO_FULL_LIMIT if limited else None
    lo, hi = day_bounds(start, end)
    items = overlapping(loggers, start, end)
    if resolution == "full":
        mode = "full"
        parts = fetch_each(client.raw, items)
        joined = join.join([(lg, r.series) for lg, r in parts])
        rows = figures.rows(joined.series)
        fig, keys, bin_s = figures.data_figure(rows, "raw", revision, window=(lo, hi), changes=joined.changes)
        what = _full_what(lo, hi, bin_s)
        info = _prefixed(parts, texts.INFO_KIND_RAW)
        # the profile: one /daily request set per logger with data in the window, in turn
        profile_parts = []
        try:
            for lg, a, b in items:
                profile_parts.append((lg, client.daily(lg["value"], a, b)))
            pj = join.join([(lg, r.series) for lg, r in profile_parts])
            out["profile"] = profile.store_data(api_client.ground_temperature(pj.series), logger_value)
        except api_client.ApiError as e:        # the client logged it
            out["profile"] = profile.empty(logger_value, str(e))
        info += _prefixed(profile_parts, texts.INFO_KIND_PROFILE)
    else:
        mode = "daily"
        parts = fetch_each(client.daily, items)
        joined = join.join([(lg, r.series) for lg, r in parts])
        rows = figures.rows(joined.series)
        fig, keys, _ = figures.data_figure(rows, "daily", revision, changes=joined.changes)
        what = texts.INFO_DAILY.format(start=start.isoformat(), end=end.isoformat())
        info = _prefixed(parts)
        out["profile"] = profile.store_data(api_client.ground_temperature(joined.series), logger_value)
    if profile.has_profile(out["profile"]):
        out["profile"]["changes"] = joined.changes
    names = [join.short_label(lg) for lg, _ in parts]
    n_requests = sum(len(r.requests) for _, r in info)
    logger.info("v2 load %s %s %s..%s: %d logger(s), %d request(s), API %.2f s, %d bytes, %d rows, "
                "%d traces, %.2f s", logger_value, mode, lo.isoformat(), hi.isoformat(), len(parts),
                n_requests, sum(r.wall_seconds for _, r in info), sum(r.bytes for _, r in info),
                len(rows), len(keys), time.perf_counter() - t0)
    out["info"] = info_parts(what, info, note, names)
    if not rows:
        out["message"] = _alert(texts.NO_DATA_ALL.format(start=start.isoformat(), end=end.isoformat()), "info")
        return out
    out["figure"] = fig
    out["view"] = {"logger": logger_value, "mode": mode, "range": [lo.isoformat(), hi.isoformat()],
                   "keys": keys, "note": note, "loggers": loggers}
    return out


def zoom_all(client: api_client.Client, view, window):
    """(series, info parts, logger names) of a zoom with All loggers: /raw for the window per
    logger with data in it, one after the other, joined."""
    items = []
    for lg in view["loggers"]:
        first, last = _date(lg.get("first") or lg.get("last")), _date(lg.get("last"))
        if first is None or last is None:
            continue
        part = clip_window(window, day_bounds(first, last))
        if part is not None:
            items.append((lg, *part))
    parts = fetch_each(client.raw, items)
    joined = join.join([(lg, r.series) for lg, r in parts])
    return joined.series, _prefixed(parts, texts.INFO_KIND_RAW), [join.short_label(lg) for lg, _ in parts]


def zoom(client: api_client.Client, relayout, view):
    """(patch, info) of a zoom in Full: /raw for the window (or the stored range on
    autorange), thinned, the traces' x/y/mode replaced by key; None if nothing to do (Daily,
    no x range change). Raises ApiError."""
    if not view or view.get("mode") != "full":
        return None
    target = zoom_target(relayout)
    if target is None:
        return None
    bounds = tuple(parse_time(t) for t in view["range"])
    if target == "overview":
        window, zoomed = bounds, False
    else:
        window, zoomed = clip_window(target, bounds), True
        if window is None:
            return None
    if view.get("loggers"):         # All loggers
        series, parts, names = zoom_all(client, view, window)
    else:
        result = client.raw(view["logger"], *window)
        series, parts, names = result.series, [(None, result)], None
    by_key = {sel.key: sel for row in figures.rows(series) for sel in row.items}
    patch = Patch()
    bins = []
    for i, key in enumerate(view["keys"]):
        if key is None:
            continue
        sel = by_key.get(key)
        if sel is None:
            d = {"x": [], "y": [], "mode": "lines", "bin_seconds": None}
        else:
            d = figures.raw_trace_data(sel, *window)
        if d["bin_seconds"] is not None:
            bins.append(d["bin_seconds"])
        patch["data"][i]["x"] = d["x"]
        patch["data"][i]["y"] = d["y"]
        patch["data"][i]["mode"] = d["mode"]
    what = _full_what(window[0], window[1], max(bins) if bins else None, zoomed)
    logger.info("v2 zoom %s %s..%s: %d request(s), API %.2f s, %d bytes", view["logger"],
                window[0].isoformat(), window[1].isoformat(), sum(len(r.requests) for _, r in parts),
                sum(r.wall_seconds for _, r in parts), sum(r.bytes for _, r in parts))
    return patch, info_parts(what, parts, view.get("note"), names)


def profile_setup(store):
    """(slider min, max, marks, value, body style, note) for a new ``profile`` store: the
    loaded days, default the last one; without ground temperatures the body is hidden and
    the note says why."""
    hidden = {"display": "none"}
    if not store:
        return 0, 1, {}, None, hidden, texts.PROFILE_EMPTY
    if store.get("error"):
        return 0, 1, {}, None, hidden, _alert(texts.PROFILE_ERROR.format(reason=store["error"]), "warning")
    if not profile.has_profile(store):
        return 0, 1, {}, None, hidden, texts.PROFILE_NO_GROUND
    first, last = store["first"], profile.last_day(store)
    return first, last, profile.slider_marks(first, last), last, {}, None


def profile_label(day: dict, annual, store, station=None) -> str:
    text = texts.PROFILE_LABEL.format(day=day["day"])
    if station:
        text += texts.PROFILE_LABEL_STATION.format(station=station)
    if annual is not None:
        text += "; " + texts.PROFILE_LABEL_ANNUAL.format(start=annual["start"], end=annual["end"],
                                                          valid=annual["valid_days"], days=annual["days"])
        if annual["days"] < profile.WINDOW_DAYS:
            text += texts.PROFILE_LABEL_SHORT.format(start=profile.day_text(store["first"]))
    return text


def profile_draw(day_value, envelope, store):
    """(profile figure, label, day marker patch of the data figure) of a day and envelope,
    from the store alone (no API request); None without a profile."""
    if not profile.has_profile(store):
        return None
    day = profile.clamp_day(store, day_value)
    envelope = envelope if envelope in figures.ENVELOPES else "day"
    depth_max = max(store["depths"])
    drawn, station = profile.for_day(store, day)     # several stations: the day's one
    p = profile.day_profile(drawn, day)
    annual = profile.annual_envelope(drawn, day) if envelope in ("annual", "both") else None
    fig = figures.profile_figure(p, annual, envelope, store.get("x_range"), depth_max,
                                 revision=str(store.get("logger")))
    marker = Patch()    # the day, and the logger change lines (All loggers) kept
    marker["layout"]["shapes"] = [figures.day_marker(p["day"])] + figures.change_shapes(store.get("changes"))
    return fig, profile_label(p, annual, store, station), marker


# ---- registration -----------------------------------------------------------------------------

def register(app, client: api_client.Client):
    """Register the page's callbacks on ``app`` (the v2 Dash instance), using ``client``."""

    @app.callback(
        Output("masts", "data"),
        Output("mast", "options"),
        Output("mast", "value"),
        Output("masts-message", "children"),
        Input("url", "pathname"),
    )
    def load_masts(_pathname):
        try:
            masts = client.masts()
        except api_client.ApiError as e:        # the client logged it
            return None, [], None, _alert(texts.MASTS_UNAVAILABLE.format(reason=str(e)), "danger")
        if not masts:
            return [], [], None, _alert(texts.NO_MASTS, "warning")
        options = [{"label": m["name"], "value": m["name"]} for m in masts]
        return masts, options, masts[0]["name"], None

    @app.callback(
        Output("logger", "options"),
        Output("logger", "value"),
        Input("mast", "value"),
        State("masts", "data"),
    )
    def fill_loggers(mast_name, masts):
        mast = find_mast(masts, mast_name)
        if mast is None:
            return [], None
        default = api_client.default_logger(mast)
        return logger_options(mast), default["value"] if default else None

    @app.callback(
        Output("period", "min_date_allowed"),
        Output("period", "max_date_allowed"),
        Output("period", "start_date"),
        Output("period", "end_date"),
        Output("period", "initial_visible_month"),
        Input("logger", "value"),
        State("masts", "data"),
    )
    def set_period(logger_value, masts):
        period = logger_period(masts, logger_value)
        if period is None:
            return None, None, None, None, None
        first, last = period
        return first, last, first, last, last

    @app.callback(
        Output("data-graph", "figure"),
        Output("info", "children"),
        Output("message", "children"),
        Output("view", "data"),
        Output("profile", "data"),
        Input("logger", "value"),
        Input("period", "start_date"),
        Input("period", "end_date"),
        Input("resolution", "value"),
        State("masts", "data"),
    )
    def load_data(logger_value, start, end, resolution, masts):
        try:
            r = load(client, logger_value, start, end, resolution, masts)
        except api_client.ApiError as e:        # the client logged it
            return figures.empty_figure(""), None, _alert(str(e), "danger"), None, None
        except Exception:   # a bug or an unexpected payload: message, log, page stays usable
            logger.exception("v2 load %s %s..%s (%s) failed", logger_value, start, end, resolution)
            return figures.empty_figure(""), None, _alert(texts.UNEXPECTED, "danger"), None, None
        return r["figure"], r["info"], r["message"], r["view"], r["profile"]

    @app.callback(
        Output("data-graph", "figure", allow_duplicate=True),
        Output("info", "children", allow_duplicate=True),
        Output("message", "children", allow_duplicate=True),
        Input("data-graph", "relayoutData"),
        State("view", "data"),
        prevent_initial_call=True,
    )
    def zoom_data(relayout, view):
        try:
            r = zoom(client, relayout, view)
        except api_client.ApiError as e:        # the client logged it
            return no_update, no_update, _alert(str(e), "danger")
        except Exception:
            logger.exception("v2 zoom failed (%s)", relayout)
            return no_update, no_update, _alert(texts.UNEXPECTED, "danger")
        if r is None:
            return no_update, no_update, no_update
        return r[0], r[1], None

    @app.callback(
        Output("profile-day", "min"),
        Output("profile-day", "max"),
        Output("profile-day", "marks"),
        Output("profile-day", "value"),
        Output("profile-body", "style"),
        Output("profile-note", "children"),
        Input("profile", "data"),
    )
    def setup_profile(store):
        try:
            return profile_setup(store)
        except Exception:
            logger.exception("v2 profile setup failed")
            return 0, 1, {}, None, {"display": "none"}, _alert(texts.UNEXPECTED, "danger")

    @app.callback(
        Output("profile-graph", "figure"),
        Output("profile-label", "children"),
        Output("data-graph", "figure", allow_duplicate=True),
        Input("profile-day", "value"),
        Input("profile-envelope", "value"),
        Input("profile", "data"),
        prevent_initial_call=True,
    )
    def draw_profile(day_value, envelope, store):
        try:
            r = profile_draw(day_value, envelope, store)
        except Exception:
            logger.exception("v2 profile draw failed (day %s, %s)", day_value, envelope)
            return figures.empty_figure(texts.UNEXPECTED, figures.PROFILE_HEIGHT), None, no_update
        if r is None:
            return figures.empty_figure("", figures.PROFILE_HEIGHT), None, no_update
        return r

    return app
