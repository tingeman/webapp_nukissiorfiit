"""Phase 2 ("new") page of webapp_nuki, at /app/nuki/v2/ (docs/plan/PLAN_api-v2.md, T7 on).

A second Dash instance on the Flask server of the phase 1 ("classic") page in app.py, so both
pages run in the same gunicorn process: ``app.py`` calls ``create_app(server)`` once, after
its own Dash app is defined. The two instances have separate layouts, component ids and
callback maps. Callbacks are registered with ``app.callback`` on this instance only (see
callbacks.py), never with the global ``dash.callback``: Dash copies global callbacks into the
first app that sets itself up (on the first request to the server, whatever its path).

The package is imported as ``v2`` (app/ is the module root: WORKDIR /app in the image, /app on
sys.path in the unit tests). Its modules import each other relatively, so their names
(layout, callbacks, texts) cannot collide with top-level modules of app/.

Static files: app/v2/assets/ (Bootstrap 5.3.8 CSS, vendored, style.css, and profile.js,
the date tooltip of the profile's day slider), served by this
Dash app only, under /app/nuki/v2/assets/ (the classic app's assets folder is app/assets/,
which does not exist, so the classic page is unaffected). plotly.js and the Dash renderer
come from the installed packages (serve_locally): the page loads nothing from a third-party
server.

One API client per process (``create_app`` makes it from the settings unless one is given):
its constructor only sets plain attributes and a lock, so it is safe with gunicorn --preload.
"""
from pathlib import Path

from dash import Dash

from . import api_client, callbacks, layout, texts

URL_PREFIX = "/app/nuki/v2/"
CLASSIC_URL = "/app/nuki/"
ASSETS = Path(__file__).resolve().parent / "assets"


def create_app(server, client: api_client.Client | None = None) -> Dash:
    """The phase 2 Dash app on the given Flask server, under URL_PREFIX. ``client``: the API
    client (default: one from config.settings)."""
    if client is None:
        import config       # app/config.py (top-level module); reads the env file at import
        client = api_client.Client.from_settings(config.settings)
    dash_app = Dash(
        __name__,
        server=server,
        title=texts.PAGE_TITLE,
        update_title=None,
        requests_pathname_prefix=URL_PREFIX,
        routes_pathname_prefix=URL_PREFIX,
        assets_folder=str(ASSETS),
        serve_locally=True,
        meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    )
    dash_app.layout = layout.build(CLASSIC_URL)
    callbacks.register(dash_app, client)
    return dash_app
