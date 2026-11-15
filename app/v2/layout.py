"""Layout of the phase 2 page (docs/plan/PLAN_api-v2.md, T9). It makes no API call: the mast
list is filled by a callback after the page has loaded, so GET /app/nuki/v2/ never depends on
the API.

Component ids (used by callbacks.py and the tests):

    classic-link     link back to the classic page (/app/nuki/)
    url              page location; its first render loads the mast catalogue
    masts            dcc.Store: the mast list (api_client.Client.masts()); a mast gets its
                     "loggers" (Client.mast()) when it is chosen
    masts-message    problem with the mast list or the chosen mast's loggers
    mast, logger     dropdowns (logger default: the one with the latest data; "All loggers",
                     value "all:<mast>", is the last option of a mast with 2 or more)
    period           DatePickerRange: limits and default = the logger's first..last day;
                     not persisted (the logger callback sets it on every load)
    resolution       "daily" (default) / "full"; persisted per browser session
    message          problem of the last data load (an alert)
    data-heading     what the figure shows: mast, station(s), logger(s), Daily/Full, period
                     (and the problem when nothing is drawn); written with the figure
    info             what the figure shows, and the requests it took
    data-graph       the figure (one make_subplots figure, shared x axes)
    view             dcc.Store: what the figure shows (callbacks.py), for the zoom
    profile          dcc.Store: the daily ground temperatures of the load (profile.py), so
                     the profile is drawn for any day and envelope without an API request
    profile-note     why there is no profile (no data loaded, no ground temperatures, ...)
    profile-body     slider, envelope choice, label and profile figure; hidden without data
    profile-day      day slider; value = days since 1970-01-01 (its tooltip shows the date,
                     assets/profile.js); limits = the loaded days, default the last one
                     (set by the load callback); updatemode "drag": the profile follows
                     the slider while it is dragged (drawn in the browser, no request)
    profile-envelope "day" (default) / "annual" / "both"; persisted per browser session
    profile-label    the chosen day and the annual envelope's window
    profile-graph    temperature against depth of the chosen day

    download-from, download-to  first and last calendar year of the download (dropdowns;
                     options and default: the first..last year with data of the loggers
                     ticked in download-loggers)
    profile-card, download-card  the two cards, side by side (2/3 and 1/3 width, not
                     stretched to equal height) on lg+ screens, stacked below
    download-all     checklist with the one option "Toggle all recording devices at this
                     station" (value "all"), above download-loggers;
                     ticking it ticks every logger, unticking it none; it is ticked exactly
                     when every logger is
    download-loggers checklist of the mast's loggers; default all; reset when the mast changes
    download-button  starts the download; disabled while one runs
    download-message what the last download did (a problem, or the file's name and size)
    download-file    dcc.Download that hands the .xlsx to the browser
"""
import dash_bootstrap_components as dbc
from dash import dcc, html

from . import figures, texts

CONFIG = {"displaylogo": False, "responsive": True}


def _field(label: str, control, **col):
    return dbc.Col([dbc.Label(label, className="nuki-label"), control], **col)


def controls():
    resolution = dbc.RadioItems(
        id="resolution", value="daily", persistence=True, persistence_type="session",
        options=[{"label": texts.RES_DAILY, "value": "daily"},
                 {"label": texts.RES_FULL, "value": "full"}],
        className="btn-group", inputClassName="btn-check",
        labelClassName="btn btn-outline-primary", labelCheckedClassName="active")
    return dbc.Card(dbc.CardBody(dbc.Row([
        _field(texts.MAST, dcc.Dropdown(id="mast", options=[], placeholder=texts.MAST_PLACEHOLDER,
                                        clearable=False), xs=12, md=6, xl=3),
        _field(texts.LOGGER, dcc.Dropdown(id="logger", options=[], placeholder=texts.LOGGER_PLACEHOLDER,
                                          clearable=False), xs=12, md=6, xl=4),
        _field(texts.PERIOD, dcc.DatePickerRange(
            id="period", display_format="YYYY-MM-DD", first_day_of_week=1, clearable=False,
            updatemode="bothdates", className="nuki-period"), xs=12, md=6, xl=3),
        _field(texts.RESOLUTION, html.Div(resolution, title=texts.RESOLUTION_HELP), xs=12, md=6, xl=2),
    ], className="g-3 align-items-start")), className="nuki-card mb-3")


def figure_section():
    return dbc.Card(dbc.CardBody([
        # the heading says what the figure shows; written with the figure (same callback), so
        # it never describes other data; the spinner marks it while a new load runs
        dcc.Loading([html.H2(id="data-heading", className="nuki-heading"),
                     html.Div(id="info", className="nuki-info")], type="dot", delay_show=200,
                    overlay_style={"visibility": "visible"}),
        dcc.Loading(dcc.Graph(id="data-graph", config=CONFIG,
                              figure=figures.empty_figure(texts.FIGURE_EMPTY)),
                    type="circle", delay_show=300),
    ]), className="nuki-card mb-3")


def profile_section():
    envelope = dbc.RadioItems(
        id="profile-envelope", value="day", persistence=True, persistence_type="session",
        options=[{"label": texts.ENV_DAY, "value": "day"},
                 {"label": texts.ENV_ANNUAL, "value": "annual"},
                 {"label": texts.ENV_BOTH, "value": "both"}],
        className="btn-group", inputClassName="btn-check",
        labelClassName="btn btn-outline-primary btn-sm", labelCheckedClassName="active")
    slider = dcc.Slider(
        id="profile-day", min=0, max=1, step=1, value=None, marks={}, included=False,
        allow_direct_input=False, updatemode="drag",
        tooltip={"placement": "bottom", "always_visible": True, "transform": "nukiDay"})
    return dbc.Card(dbc.CardBody([
        html.Div(texts.PROFILE_TITLE, className="nuki-card-title"),
        html.Div(texts.PROFILE_EMPTY, id="profile-note", className="nuki-profile-note"),
        html.Div([
            dbc.Row([          # stacked: the card is half the page wide on large screens
                _field(texts.PROFILE_DAY, html.Div(slider, className="nuki-profile-slider"), xs=12),
                _field(texts.PROFILE_ENVELOPE, html.Div(envelope, title=texts.ENVELOPE_HELP), xs=12),
            ], className="g-2 mb-2"),
            html.Div(id="profile-label", className="nuki-info"),
            # explicit height: the graph is created inside the hidden profile-body, and
            # without it its div keeps height 0 when shown (the plot then overlaps the next card)
            dcc.Loading(dcc.Graph(id="profile-graph", config=CONFIG,
                                  style={"height": f"{figures.PROFILE_HEIGHT}px"},
                                  figure=figures.empty_figure("", figures.PROFILE_HEIGHT)),
                        type="circle", delay_show=300),
        ], id="profile-body", style={"display": "none"}),
    ]), id="profile-card", className="nuki-card")


def download_section():
    """The download card, for the narrow right-hand column on large screens: everything
    stacked, top to bottom: title, intro, the year dropdowns (side by side where the column
    is wide enough: below lg, where the card is full width, and on xl), the "toggle all" box,
    the logger checklist, the button directly below, then its message."""
    return dbc.Card(dbc.CardBody([
        html.Div(texts.DOWNLOAD_TITLE, className="nuki-card-title"),
        html.P(texts.DOWNLOAD_INTRO, className="nuki-download-intro"),
        dbc.Row([
            _field(texts.DOWNLOAD_FROM, dcc.Dropdown(id="download-from", options=[], clearable=False),
                   xs=6, lg=12, xl=6),
            _field(texts.DOWNLOAD_TO, dcc.Dropdown(id="download-to", options=[], clearable=False),
                   xs=6, lg=12, xl=6),
        ], className="g-2 mb-3"),
        html.Div([
            dbc.Label(texts.DOWNLOAD_LOGGERS, className="nuki-label"),
            dbc.Checklist(id="download-all", options=[{"label": texts.DOWNLOAD_ALL, "value": "all"}],
                          value=["all"], className="nuki-download-all"),
            dbc.Checklist(id="download-loggers", options=[], value=[], className="nuki-download-loggers"),
        ], className="mb-3"),
        dbc.Button(texts.DOWNLOAD_BUTTON, id="download-button", color="primary", n_clicks=0),
        html.Div(id="download-message", className="nuki-download-message mt-2"),
        dcc.Download(id="download-file"),
    ]), id="download-card", className="nuki-card")


def cards():
    """The profile card (two thirds) and the download card (one third) side by side on large
    screens (lg and up), each only as tall as its content (the row aligns them at the top);
    both full width, the download below the profile, on smaller screens."""
    return dbc.Row([
        dbc.Col(profile_section(), xs=12, lg=8),
        dbc.Col(download_section(), xs=12, lg=4),
    ], className="g-3 mb-3 align-items-start")


def build(classic_url):
    """The page. ``classic_url`` is the absolute path of the classic page; a plain link (full
    page load), since the classic page is another Dash app."""
    return html.Div([
        dcc.Location(id="url", refresh=False),
        dcc.Store(id="masts"),
        dcc.Store(id="view"),
        dcc.Store(id="profile"),
        html.Header(dbc.Container([
            dbc.Row([
                dbc.Col(html.H1(texts.TITLE, className="nuki-title"), xs=12, lg=True),
                dbc.Col(html.A(texts.BACK_LINK, id="classic-link", href=classic_url),
                        width="auto", className="align-self-center"),
            ], className="g-2"),
            html.Div([dbc.Badge(texts.BADGE, color="warning", className="text-dark me-2"),
                      html.Span(texts.SUBTITLE, className="nuki-subtitle")]),
        ], fluid="xl"), className="nuki-header"),
        dbc.Container([
            html.P(texts.INTRO, className="nuki-intro"),
            html.Div(id="masts-message"),
            controls(),
            html.Div(id="message"),
            figure_section(),
            cards(),
            html.Footer(texts.FOOTER, className="nuki-footer"),
        ], fluid="xl", className="nuki-main"),
    ])
