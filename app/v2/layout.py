"""Layout of the phase 2 page (docs/plan/PLAN_api-v2.md, T9). It makes no API call: the mast
list is filled by a callback after the page has loaded, so GET /app/nuki/v2/ never depends on
the API.

Component ids (used by callbacks.py and the tests):

    classic-link     link back to the classic page (/app/nuki/)
    url              page location; its first render loads the mast catalogue
    masts            dcc.Store: the catalogue (api_client.Client.masts())
    masts-message    problem with the mast list
    mast, logger     dropdowns (logger default: the one with the latest data)
    period           DatePickerRange: limits and default = the logger's first..last day;
                     not persisted (the logger callback sets it on every load)
    resolution       "daily" (default) / "full"; persisted per browser session
    message          problem of the last data load (an alert)
    info             what the figure shows, and the requests it took
    data-graph       the figure (one make_subplots figure, shared x axes)
    view             dcc.Store: what the figure shows (callbacks.py), for the zoom

Room for the next tasks: the profile (T10) goes in its own card below the figure
(``profile_section``), "All loggers" (T11) is an option of the logger dropdown, and the
download (T12) a card of its own after the profile.
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
        dcc.Loading(html.Div(id="info", className="nuki-info"), type="dot", delay_show=200,
                    overlay_style={"visibility": "visible"}),
        dcc.Loading(dcc.Graph(id="data-graph", config=CONFIG,
                              figure=figures.empty_figure(texts.FIGURE_EMPTY)),
                    type="circle", delay_show=300),
    ]), className="nuki-card mb-3")


def build(classic_url):
    """The page. ``classic_url`` is the absolute path of the classic page; a plain link (full
    page load), since the classic page is another Dash app."""
    return html.Div([
        dcc.Location(id="url", refresh=False),
        dcc.Store(id="masts"),
        dcc.Store(id="view"),
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
            # T10: profile_section() here; T12: the download card after it
            html.Footer(texts.FOOTER, className="nuki-footer"),
        ], fluid="xl", className="nuki-main"),
    ])
