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
   column key of each trace (None for band traces), "note"}.
5. Zoom (``data-graph.relayoutData``) in Full: /raw for the visible window only (one request
   set, exactly the window's datetimes), thinned and patched into the traces by key (the
   figure's zoom stays as the user left it); autorange (reset) loads the stored range again.
   Daily makes no request on a zoom. Nothing is kept on the server between requests.

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

from . import api_client, figures, texts

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


def info_items(what: str, result, note: str | None = None):
    """The info line: what the figure shows, the API part (requests, wall seconds, size) and
    the requests one by one (folded)."""
    api = texts.INFO_REQUESTS.format(requests=len(result.requests), seconds=result.wall_seconds,
                                     size=size_text(result.bytes))
    lines = [html.Span(what, className="nuki-info-what"), html.Span(" · " + api, className="nuki-info-api")]
    out = [html.Div(lines)]
    if note:
        out.append(html.Div(note, className="nuki-info-note"))
    out.append(html.Details([
        html.Summary(texts.INFO_DETAILS),
        html.Ul([html.Li(texts.INFO_REQUEST_LINE.format(start=r.start, end=r.end, seconds=r.seconds,
                                                        size=size_text(r.bytes)))
                 for r in result.requests], className="nuki-requests"),
    ], className="nuki-details"))
    return out


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


def load(client: api_client.Client, logger_value, start, end, resolution):
    """Everything a logger/period/resolution change produces, as plain values: figure,
    info (children), message (alert or None), view (store or None). Raises ApiError."""
    out = {"figure": figures.empty_figure(texts.FIGURE_EMPTY), "info": None, "message": None,
           "view": None}
    if not logger_value:
        return out
    start, end = _date(start), _date(end)
    if start is None or end is None:
        out["message"] = _alert(texts.CHOOSE_PERIOD, "warning")
        return out
    if start > end:
        out["message"] = _alert(texts.PERIOD_REVERSED, "warning")
        return out
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
    else:
        lo, hi = day_bounds(start, end)
        result = client.daily(logger_value, start, end)
        rows = figures.rows(result.series)
        fig, keys, _ = figures.data_figure(rows, "daily", revision)
        note = None
        what = texts.INFO_DAILY.format(start=start.isoformat(), end=end.isoformat())
        mode = "daily"
    logger.info("v2 load %s %s %s..%s: %d request(s), API %.2f s, %d bytes, %d rows, %d traces, %.2f s",
                logger_value, mode, lo.isoformat(), hi.isoformat(), len(result.requests),
                result.wall_seconds, result.bytes, len(rows), len(keys), time.perf_counter() - t0)
    if not rows:
        out["message"] = _alert(texts.NO_DATA.format(start=start.isoformat(), end=end.isoformat()), "info")
        out["info"] = info_items(what, result, note)
        return out
    out["figure"] = fig
    out["info"] = info_items(what, result, note)
    out["view"] = {"logger": logger_value, "mode": mode, "range": [lo.isoformat(), hi.isoformat()],
                   "keys": keys, "note": note}
    return out


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
    result = client.raw(view["logger"], *window)
    by_key = {sel.key: sel for row in figures.rows(result.series) for sel in row.items}
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
                window[0].isoformat(), window[1].isoformat(), len(result.requests),
                result.wall_seconds, result.bytes)
    return patch, info_items(what, result, view.get("note"))


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
        options = [{"label": lg["label"], "value": lg["value"]} for lg in mast["loggers"]]
        return options, default["value"] if default else None

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
        lg = find_logger(masts, logger_value)
        if lg is None or not lg.get("last"):
            return None, None, None, None, None
        first, last = (lg.get("first") or lg["last"])[:10], lg["last"][:10]
        return first, last, first, last, last

    @app.callback(
        Output("data-graph", "figure"),
        Output("info", "children"),
        Output("message", "children"),
        Output("view", "data"),
        Input("logger", "value"),
        Input("period", "start_date"),
        Input("period", "end_date"),
        Input("resolution", "value"),
    )
    def load_data(logger_value, start, end, resolution):
        try:
            r = load(client, logger_value, start, end, resolution)
        except api_client.ApiError as e:        # the client logged it
            return figures.empty_figure(""), None, _alert(str(e), "danger"), None
        except Exception:   # a bug or an unexpected payload: message, log, page stays usable
            logger.exception("v2 load %s %s..%s (%s) failed", logger_value, start, end, resolution)
            return figures.empty_figure(""), None, _alert(texts.UNEXPECTED, "danger"), None
        return r["figure"], r["info"], r["message"], r["view"]

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

    return app
