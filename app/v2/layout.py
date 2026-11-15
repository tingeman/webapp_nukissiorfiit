"""Layout of the phase 2 page. Static for now (no data features yet; the content is designed
in its own planning session). It makes no API call, so GET /app/nuki/v2/ never depends on it.

Component ids (used by callbacks.py and the tests):

    classic-link    link back to the classic page (/app/nuki/)
"""
import dash_bootstrap_components as dbc
from dash import html

from . import texts


def build(classic_url):
    """The page. ``classic_url`` is the absolute path of the classic page; a plain link (full
    page load), since the classic page is another Dash app."""
    return html.Div(
        [
            dbc.Row(dbc.Col(html.H1(texts.TITLE), width="auto")),
            dbc.Row(dbc.Col(dbc.Badge(texts.BADGE, color="warning", className="text-dark"), width="auto"),
                    className="mb-3"),
            html.P(texts.INTRO),
            html.A(texts.BACK_LINK, id="classic-link", href=classic_url),
        ],
        className="dbc p-4",
    )
