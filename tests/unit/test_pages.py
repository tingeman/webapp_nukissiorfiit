"""Unit tests of the two pages on one Flask server: the classic page (app/app.py, /app/nuki/)
and the phase 2 page (app/v2/, /app/nuki/v2/), with Flask's test client (no gunicorn, no
API call: neither page's layout requests data). Each test's docstring is its name in the
test output ("unit: pages: <docstring>")."""
import json
import re
import unittest

import support  # noqa: F401  (puts /app on sys.path)

from dash import _callback

import app  # noqa: E402
import v2  # noqa: E402


def dash_config(body):
    m = re.search(r'<script id="_dash-config" type="application/json">(.*?)</script>', body, re.S)
    return json.loads(m.group(1)) if m else {}


def components(node):
    """All components of a Dash layout JSON (as served by _dash-layout), depth first."""
    if isinstance(node, list):
        for item in node:
            yield from components(item)
    elif isinstance(node, dict):
        if "props" in node:
            yield node
            yield from components(node["props"].get("children"))
        else:
            for value in node.values():
                yield from components(value)


def layout_ids(dash_app):
    return {c.id for c in dash_app.layout._traverse() if getattr(c, "id", None)}


def callback_ids(dash_app):
    """Component ids of the outputs, inputs and states of the app's callbacks."""
    ids = set()
    for key, cb in dash_app.callback_map.items():
        for part in key.strip(".").split("..."):
            ids.add(part.rsplit(".", 1)[0])
        ids.update(d["id"] for d in cb.get("inputs", []) + cb.get("state", []))
    return ids


class PagesTest(unittest.TestCase):
    def setUp(self):
        self.client = app.server.test_client()

    def get(self, path):
        r = self.client.get(path)
        return r.status_code, r.get_data(as_text=True)

    def links(self, prefix):
        status, body = self.get(prefix + "_dash-layout")
        self.assertEqual(status, 200, body[:300])
        return {c["props"].get("id"): c["props"] for c in components(json.loads(body)) if c.get("type") == "A"}

    def test_classic_page(self):
        """GET /app/nuki/ answers 200 with the classic Dash page and the "Try the new page" link to /app/nuki/v2/"""
        status, body = self.get("/app/nuki/")
        self.assertEqual(status, 200, body[:300])
        self.assertEqual(dash_config(body).get("requests_pathname_prefix"), "/app/nuki/")
        link = self.links("/app/nuki/").get("v2-link")
        self.assertIsNotNone(link)
        self.assertEqual(link["href"], "/app/nuki/v2/")
        self.assertEqual(link["children"], "Try the new page (in development)")

    def test_v2_page(self):
        """GET /app/nuki/v2/ answers 200 with the phase 2 Dash page, the classic title, a badge and the "Back to the classic page" link"""
        status, body = self.get("/app/nuki/v2/")
        self.assertEqual(status, 200, body[:300])
        self.assertEqual(dash_config(body).get("requests_pathname_prefix"), "/app/nuki/v2/")
        link = self.links("/app/nuki/v2/").get("classic-link")
        self.assertIsNotNone(link)
        self.assertEqual(link["href"], "/app/nuki/")
        self.assertEqual(link["children"], "Back to the classic page")
        status, body = self.get("/app/nuki/v2/_dash-layout")
        tree = list(components(json.loads(body)))
        h1 = [c["props"]["children"] for c in tree if c.get("type") == "H1"]
        classic_h1 = [c.children for c in app.app.layout._traverse() if type(c).__name__ == "H1"]
        self.assertEqual(h1, classic_h1)
        self.assertIn("Badge", [c.get("type") for c in tree])

    def test_v2_app_on_classic_server(self):
        """the phase 2 app is a second Dash instance on the classic page's Flask server"""
        self.assertIsNot(app.v2_app, app.app)
        self.assertIs(app.v2_app.server, app.server)
        self.assertEqual(v2.URL_PREFIX, "/app/nuki/v2/")

    def test_callbacks_apart(self):
        """each page's callbacks are on its own Dash app only (no global dash.callback): outputs like graph.children only on the classic app"""
        # Dash copies global callbacks into an app on the first request; make sure it happened
        self.assertEqual(self.get("/app/nuki/")[0], 200)
        self.assertEqual(self.get("/app/nuki/v2/")[0], 200)
        self.assertEqual(_callback.GLOBAL_CALLBACK_MAP, {})
        self.assertEqual(_callback.GLOBAL_CALLBACK_LIST, [])
        classic, new = app.app.callback_map, app.v2_app.callback_map
        self.assertIn("..graph.children...table.rowData...table.columnDefs..", classic)
        self.assertEqual(len(classic), 4)
        self.assertFalse([k for k in new if "graph.children" in k or "catalogue" in k], new.keys())
        # every id a callback uses is in the layout of its own app
        self.assertLessEqual(callback_ids(app.app), layout_ids(app.app))
        self.assertLessEqual(callback_ids(app.v2_app), layout_ids(app.v2_app))
        # each app's callbacks are its own functions (both pages may use ids like "logger", so
        # equal keys alone do not mean a callback was copied over)
        self.assertEqual({cb["callback"].__module__ for cb in classic.values()}, {"app"})
        # (the v2 profile is drawn by a clientside callback: no Python function, T16)
        self.assertEqual({cb["callback"].__module__ for cb in new.values() if "callback" in cb}, {"v2.callbacks"})

    def test_no_global_callback_decorator(self):
        """app.py and app/v2/ register callbacks with app.callback, never the global dash.callback"""
        # Dash copies global callbacks into the first app that sets itself up (today the
        # classic one, whatever the path of the first request): fragile with two apps.
        files = [support.APP_DIR / "app.py"] + sorted((support.APP_DIR / "v2").glob("*.py"))
        pattern = re.compile(r"^\s*@(dash\.)?callback\b|^\s*from dash import .*\bcallback\b", re.M)
        for path in files:
            self.assertIsNone(pattern.search(path.read_text(encoding="utf-8")), path)


if __name__ == "__main__":
    unittest.main()
