"""Callbacks of the phase 2 page. None yet.

``register(app)`` attaches them with ``app.callback`` to the phase 2 Dash instance only. Do not
use the global ``dash.callback`` decorator here (or in app.py): with two Dash apps on one
server, Dash copies the global callbacks into the first app that sets itself up (on the first
request to the server, whatever its path), which is not necessarily the page they belong to.
"""


def register(app):
    """Register the page's callbacks on ``app`` (the v2 Dash instance)."""
    return app
