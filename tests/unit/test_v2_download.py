"""Unit tests of the Excel download of the phase 2 page (app/v2/download.py and its callbacks,
docs/plan/PLAN_api-v2.md, T12): the workbook opened with openpyxl (sheet names, headers, row
counts and values against the stub monitoring_db API's own /raw and /daily answers for the
fixtures), the request pattern (one logger after the other, at most max_parallel in flight, no
request for a logger outside the years), the time budget (a slow stub and a small budget: no
chunk is started after it, and the "too large" text), sheet names (at most 31 characters,
unique), the filename, the checklists and year selectors, and the download callback over HTTP
(dcc.send_bytes data). Fixtures: tests/fixtures/make_fixtures.py. Each test's docstring is its
name in the test output ("unit: v2_download: <docstring>")."""
import base64
import datetime as dt
import io
import unittest

import openpyxl

from support import ApiTestCase, config, fake_api
from test_v2_page import CAMPBELL_M6, TEKBOX_M6, PageTestCase, text_of

import app  # noqa: E402
import v2  # noqa: E402
from v2 import api_client, callbacks, download, texts  # noqa: E402
from v2.api_client import Client  # noqa: E402

TEKBOX_M9 = "deveui:feedbeefcafe0001"          # device 20, SIS2024-03, 2024-10-14..18
CAMPBELL_M9 = "serial:NUKI2026-M9:12215"       # device 30, NUKI2026-M9, 2026-10-01..05
NOW = dt.datetime(2026, 11, 15, 12, 0, 0, tzinfo=dt.timezone.utc)
M6_SHEETS = ["Info", "Tekbox_TBSL1_3 cafe0002 full", "Tekbox_TBSL1_3 cafe0002 daily",
             "CR300_M6 (12096) full", "CR300_M6 (12096) daily"]


def selector(params):
    if params.get("deveui"):
        return "deveui:" + params["deveui"]
    return f"serial:{params['station']}:{params['serial']}"


def rows(ws):
    return [list(r) for r in ws.iter_rows(values_only=True)]


def open_xlsx(content: bytes):
    return openpyxl.load_workbook(io.BytesIO(content))


def stub_answer(server, kind, logger_value, start, end):
    """The stub's own /raw or /daily JSON for a logger and days (in-process, no request)."""
    params = dict(api_client.logger_params(logger_value), start=f"{start}T00:00:00Z", end=f"{end}T23:59:59Z")
    handler = fake_api.measurements_raw if kind == "raw" else fake_api.measurements_daily
    status, body = handler(server.fixtures, params)
    assert status == 200, body
    return body


def as_time(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "")).replace(microsecond=0)


class DownloadCase(ApiTestCase):
    def setUp(self):
        super().setUp()
        self.set_settings(backend_api_daily_chunk_years=3)
        self.client = Client.from_settings(config.settings)
        self.catalogue = self.client.masts()
        self.server.requests.clear()
        self.server.max_in_flight = 0

    def mast(self, name):
        return next(m for m in self.catalogue if m["name"] == name)

    def export(self, mast, values, start, end, budget=60.0, delay=0.0):
        self.server.delay = delay
        try:
            return download.export(self.client, mast, values, start, end, budget, now=NOW)
        finally:
            self.server.delay = 0.0

    def measurements(self):
        """[(kind, logger value, start, end)] of the measurement requests, in order."""
        return [(path.rsplit("/", 1)[-1], selector(p), p["start"], p["end"])
                for path, p in self.server.requests if "/measurements/" in path]


# ---- workbook ---------------------------------------------------------------------------------

class WorkbookTest(DownloadCase):
    def setUp(self):
        super().setUp()
        mast = self.mast("Mast 6")
        self.result = self.export(mast, [lg["value"] for lg in mast["loggers"]], 2026, 2026)
        self.wb = open_xlsx(self.result.content)

    def test_sheets(self):
        """Mast 6, All, 2026: sheets Info, then "<logger> full" and "<logger> daily" per logger, in catalogue order"""
        self.assertEqual(self.wb.sheetnames, M6_SHEETS)
        self.assertEqual(self.result.filename, "nuki_Mast_6_2026-2026.xlsx")
        self.assertEqual([lg["value"] for lg in self.result.loggers], [TEKBOX_M6, CAMPBELL_M6])

    def test_full_sheets(self):
        """each full sheet: "Time (UTC)" and one column per series, every measurement of the stub's /raw (times, values, empty cells)"""
        for (value, start, end), name in zip([(TEKBOX_M6, "2026-09-01", "2026-09-05"),
                                              (CAMPBELL_M6, "2026-10-01", "2026-10-05")],
                                             (M6_SHEETS[1], M6_SHEETS[3])):
            got = rows(self.wb[name])
            body = stub_answer(self.server, "raw", value, start, end)
            self.assertEqual(got[0], [texts.XLSX_TIME] + [download.header(c) for c in body["columns"]])
            self.assertEqual(len(got) - 1, len(body["time"]), name)
            self.assertEqual(len(got) - 1, 33)     # 3-hourly, 5 days, both ends included
            self.assertEqual([r[0] for r in got[1:]], [as_time(t) for t in body["time"]])
            for j, col in enumerate(body["values"]):
                for i, v in enumerate(col):
                    cell = got[i + 1][j + 1]
                    if v is None:
                        self.assertIsNone(cell, (name, i, j))
                    else:
                        self.assertAlmostEqual(cell, v, places=9, msg=(name, i, j))
        # the Campbell inclinometer is 6-hourly: empty cells at 03:00
        got = rows(self.wb[M6_SHEETS[3]])
        x = got[0].index(next(h for h in got[0] if "XAngle" in str(h)))
        self.assertEqual([got[1][x] is None, got[2][x] is None], [False, True])

    def test_daily_sheets(self):
        """each daily sheet: "Date (UTC)", per series mean/min/max/count; equal to the stub's /daily"""
        for (value, start, end), name in zip([(TEKBOX_M6, "2026-09-01", "2026-09-05"),
                                              (CAMPBELL_M6, "2026-10-01", "2026-10-05")],
                                             (M6_SHEETS[2], M6_SHEETS[4])):
            got = rows(self.wb[name])
            body = stub_answer(self.server, "daily", value, start, end)
            heads = [texts.XLSX_DATE]
            for c in body["columns"]:
                heads += [f"{download.header(c)} {s}" for s in download.DAILY_STATS]
            self.assertEqual(got[0], heads)
            self.assertEqual(len(got) - 1, len(body["time"]))
            self.assertEqual(len(got) - 1, 5)
            self.assertEqual([r[0].date() for r in got[1:]], [dt.date.fromisoformat(d) for d in body["time"]])
            for j in range(len(body["columns"])):
                for i in range(len(body["time"])):
                    count = body["values"]["count"][j][i]
                    cells = got[i + 1][1 + 4 * j: 5 + 4 * j]
                    if not count:
                        self.assertEqual(cells, [None] * 4)
                        continue
                    for k, stat in enumerate(("mean", "min", "max")):
                        self.assertAlmostEqual(cells[k], body["values"][stat][j][i], places=9)
                    self.assertEqual(cells[3], count)
                    self.assertIsInstance(cells[3], int)

    def test_pressure_in_api_unit(self):
        """pressure stays in the API's unit (Pa, raw values, not kPa), and the Info sheet says so"""
        got = rows(self.wb[M6_SHEETS[3]])
        col = next(i for i, h in enumerate(got[0]) if "BarometricPressure" in str(h))
        self.assertTrue(got[0][col].endswith("(Pa)"), got[0][col])
        self.assertAlmostEqual(got[1][col], 101300.0)
        info = rows(self.wb["Info"])
        pressure = [r for r in info if r[4] == "BarometricPressure"]
        self.assertTrue(pressure)
        for r in pressure:
            self.assertEqual(r[5], "Pa")
            self.assertIn("not converted to kPa", r[10])

    def test_info_sheet(self):
        """Info: mast, years, download time (UTC), loggers with labels and periods, one detail row per data column"""
        info = rows(self.wb["Info"])
        by_key = {r[0]: r[1] for r in info if r and r[0]}
        self.assertEqual(by_key[texts.XLSX_MAST], "Mast 6")
        self.assertEqual(by_key[texts.XLSX_YEARS], texts.XLSX_YEARS_VALUE.format(start=2026, end=2026))
        self.assertEqual(by_key[texts.XLSX_DOWNLOADED], dt.datetime(2026, 11, 15, 12, 0, 0))
        start = next(i for i, r in enumerate(info) if r[0] == texts.XLSX_LOGGER_HEADERS[0]) + 1
        loggers = info[start:start + 2]
        self.assertEqual([r[2] for r in loggers], [TEKBOX_M6, CAMPBELL_M6])
        self.assertEqual(loggers[0][0], "Tekbox_TBSL1_3 (feedbeefcafe0002)")
        self.assertEqual(loggers[0][6], "2026-09-01 to 2026-09-05")
        self.assertEqual(loggers[1][7], "CR300_M6 (12096) full, CR300_M6 (12096) daily")
        start = next(i for i, r in enumerate(info) if r[0] == texts.XLSX_COLUMN_HEADERS[0]) + 1
        details = info[start:]
        for name in M6_SHEETS[1:]:
            ws_rows = rows(self.wb[name])
            mine = [r for r in details if r[0] == name]
            self.assertEqual([r[2] for r in mine], ws_rows[0], name)     # one row per column, in order
            self.assertEqual(mine[-1][1], openpyxl.utils.get_column_letter(len(ws_rows[0])))
        ground = [r for r in details if r[0] == M6_SHEETS[1] and r[4] == "GroundTemperature"]
        self.assertEqual([r[6] for r in ground], [-0.1, -0.25, -0.5])
        self.assertEqual({r[7] for r in ground}, {"Temperature"})

    def test_mast9_skips_logger_outside_years(self):
        """Mast 9, All, 2026: only the Campbell (2026) is asked and exported; the Tekbox (2024) makes no request and is listed as not exported"""
        mast = self.mast("Mast 9")
        self.server.requests.clear()
        result = self.export(mast, [TEKBOX_M9, CAMPBELL_M9], 2026, 2026)
        self.assertEqual({m[1] for m in self.measurements()}, {CAMPBELL_M9})
        self.assertEqual([m[0] for m in self.measurements()], ["raw", "daily"])
        wb = open_xlsx(result.content)
        self.assertEqual(wb.sheetnames, ["Info", "CR300_M9 (12215) full", "CR300_M9 (12215) daily"])
        info = rows(wb["Info"])
        tekbox = next(r for r in info if r[2] == TEKBOX_M9)
        self.assertEqual(tekbox[6], texts.XLSX_NOT_EXPORTED)

    def test_size(self):
        """Mast 6, All with the fixtures: the workbook is a valid zip of less than 100 KB"""
        self.assertEqual(self.result.content[:2], b"PK")
        self.assertLess(len(self.result.content), 100_000)
        print(f"xlsx Mast 6 All: {len(self.result.content)} bytes")


# ---- requests ---------------------------------------------------------------------------------

class RequestTest(DownloadCase):
    def test_logger_after_logger(self):
        """Mast 6, All: /raw then /daily of the Tekbox, then of the Campbell, each for its days only; never two requests in flight"""
        mast = self.mast("Mast 6")
        self.export(mast, [TEKBOX_M6, CAMPBELL_M6], 2026, 2026, delay=0.05)
        self.assertEqual(self.measurements(), [
            ("raw", TEKBOX_M6, "2026-09-01T00:00:00Z", "2026-09-05T23:59:59Z"),
            ("daily", TEKBOX_M6, "2026-09-01T00:00:00Z", "2026-09-05T23:59:59Z"),
            ("raw", CAMPBELL_M6, "2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z"),
            ("daily", CAMPBELL_M6, "2026-10-01T00:00:00Z", "2026-10-05T23:59:59Z")])
        self.assertEqual(self.server.max_in_flight, 1)

    def test_chunks_max_parallel(self):
        """a logger over 3 calendar years: 3 /raw chunks, at most max_parallel (2) in flight, all before the next logger's requests"""
        mast = dict(self.mast("Mast 6"))
        mast["loggers"] = [dict(mast["loggers"][0], first="2024-01-01T00:00:00Z"), mast["loggers"][1]]
        self.export(mast, [TEKBOX_M6, CAMPBELL_M6], 2024, 2026, delay=0.15)
        got = self.measurements()
        tekbox_raw = [m for m in got if m[:2] == ("raw", TEKBOX_M6)]
        self.assertEqual(sorted((m[2], m[3]) for m in tekbox_raw),     # 2 at once: any order
                         api_client.raw_chunks(dt.date(2024, 1, 1), dt.date(2026, 9, 5), 1))
        self.assertEqual(len(tekbox_raw), 3)
        self.assertEqual(self.server.max_in_flight, 2)
        order = [m[1] for m in got]
        first_campbell = order.index(CAMPBELL_M6)
        self.assertEqual(set(order[:first_campbell]), {TEKBOX_M6})
        self.assertEqual(set(order[first_campbell:]), {CAMPBELL_M6})
        kinds = [m[0] for m in got if m[1] == TEKBOX_M6]
        self.assertEqual(kinds, ["raw"] * 3 + ["daily"] * (len(kinds) - 3))

    def test_no_logger_in_years(self):
        """Mast 9, 2025 only (no logger has data then): the "no data" text, no request"""
        mast = self.mast("Mast 9")
        with self.assertRaises(download.DownloadError) as cm:
            self.export(mast, [TEKBOX_M9, CAMPBELL_M9], 2025, 2025)
        self.assertEqual(str(cm.exception), texts.DOWNLOAD_NO_DATA.format(start=2025, end=2025))
        self.assertEqual(self.server.requests, [])

    def test_choices_checked(self):
        """no logger ticked, no year or reversed years: a message each, no request"""
        mast = self.mast("Mast 6")
        for values, start, end, text in (([], 2026, 2026, texts.DOWNLOAD_CHOOSE_LOGGERS),
                                         ([TEKBOX_M6], None, 2026, texts.DOWNLOAD_CHOOSE_YEARS),
                                         ([TEKBOX_M6], 2026, 2025, texts.DOWNLOAD_YEARS_REVERSED)):
            with self.assertRaises(download.DownloadError) as cm:
                self.export(mast, values, start, end)
            self.assertEqual(str(cm.exception), text)
        self.assertEqual(self.server.requests, [])

    def test_time_budget(self):
        """a slow stub (0.25 s per request) and a 0.4 s budget: the 2 Tekbox requests run, no Campbell request is started, the "too large" text"""
        mast = self.mast("Mast 6")
        with self.assertRaises(api_client.ApiError) as cm:
            self.export(mast, [TEKBOX_M6, CAMPBELL_M6], 2026, 2026, budget=0.4, delay=0.25)
        self.assertEqual(str(cm.exception), texts.DOWNLOAD_TOO_LARGE)
        self.assertEqual([m[:2] for m in self.measurements()], [("raw", TEKBOX_M6), ("daily", TEKBOX_M6)])

    def test_time_budget_chunks(self):
        """with a used-up budget inside one logger's raw chunks, the queued chunks are not started"""
        mast = dict(self.mast("Mast 6"))
        mast["loggers"] = [dict(mast["loggers"][0], first="2022-01-01T00:00:00Z")]
        with self.assertRaises(api_client.ApiError) as cm:
            self.export(mast, [TEKBOX_M6], 2022, 2026, budget=0.2, delay=0.3)
        self.assertEqual(str(cm.exception), texts.DOWNLOAD_TOO_LARGE)
        self.assertEqual([m[0] for m in self.measurements()], ["raw", "raw"])     # max_parallel at t = 0

    def test_api_error(self):
        """an API error on the Campbell: ApiError with the API's text, no further request"""
        mast = self.mast("Mast 6")
        self.server.force_if = ({"serial": "12096", "path": "/raw"}, fake_api.QUERY_TIMEOUT)
        with self.assertRaises(api_client.ApiError) as cm:
            self.export(mast, [TEKBOX_M6, CAMPBELL_M6], 2026, 2026)
        self.assertEqual(str(cm.exception), texts.API_TIMEOUT)
        self.assertEqual([m[:2] for m in self.measurements()][-1], ("raw", CAMPBELL_M6))
        self.assertEqual(len(self.measurements()), 3)


# ---- names and selection (no API) ------------------------------------------------------------

def _lg(display, ident, value=None, first="2026-01-01T00:00:00Z", last="2026-02-01T00:00:00Z"):
    return {"value": value or f"deveui:{ident}", "label": f"{display} ({ident}), {first[:10]} to {last[:10]}",
            "station": "S", "first": first, "last": last}


class NamesTest(unittest.TestCase):
    def test_sheet_names_catalogue(self):
        """sheet names of the fixture loggers: the short label, or display name + last 8 of a long deveui"""
        names = download.sheet_names([_lg("Tekbox_TBSL1_3", "feedbeefcafe0002"), _lg("CR300_M6", "12096")])
        self.assertEqual(list(names.values()), [("Tekbox_TBSL1_3 cafe0002 full", "Tekbox_TBSL1_3 cafe0002 daily"),
                                                ("CR300_M6 (12096) full", "CR300_M6 (12096) daily")])

    def test_sheet_names_limit_unique(self):
        """sheet names are at most 31 characters, unique without case, not "Info", without the characters Excel forbids, for long, equal and odd labels"""
        loggers = [_lg("A very long display name of a logger [x]", "0123456789abcdef", "v1"),
                   _lg("A very long display name of a logger [x]", "0123456789abcdef", "v2"),
                   _lg("A very long display name of a logger [y]", "fedcba9876543210", "v3"),
                   _lg("same", "1", "v4"), _lg("SAME", "1", "v5"), _lg("Info", "", "v6"),
                   {"value": "v7", "label": "x:y/z*?", "station": "S"},
                   {"value": "v8", "label": "", "station": "S"}]
        loggers[5]["label"] = "Info"
        names = download.sheet_names(loggers)
        flat = [n for pair in names.values() for n in pair]
        self.assertEqual(len(flat), 2 * len(loggers))
        for n in flat:
            self.assertLessEqual(len(n), 31, n)
            self.assertNotRegex(n, r"[\[\]:*?/\\]")
            self.assertTrue(n.endswith((" full", " daily")))
        lowered = [n.lower() for n in flat] + ["info"]
        self.assertEqual(len(set(lowered)), len(lowered))
        # and openpyxl accepts them in one workbook
        wb = openpyxl.Workbook(write_only=True)
        for n in ["Info"] + flat:
            wb.create_sheet(n)
        wb.save(io.BytesIO())

    def test_filename(self):
        """filename nuki_<mast>_<from>-<to>.xlsx with safe characters only"""
        self.assertEqual(download.filename("Mast 6", 2025, 2026), "nuki_Mast_6_2025-2026.xlsx")
        self.assertEqual(download.filename("Mast 9 / Ø ../x", 2024, 2024), "nuki_Mast_9_x_2024-2024.xlsx")
        self.assertEqual(download.filename("", 2024, 2025), "nuki_mast_2024-2025.xlsx")

    def test_years(self):
        """year span and loggers in the years: calendar years clipped to each logger's period"""
        a = _lg("A", "1", first="2024-10-14T00:00:00Z", last="2024-10-18T21:00:00Z")
        b = _lg("B", "2", first="2025-12-30T00:00:00Z", last="2026-10-05T21:00:00Z")
        self.assertEqual(download.year_span([a, b]), (2024, 2026))
        self.assertIsNone(download.year_span([]))
        self.assertEqual(download.in_years([a, b], 2026, 2026), [(b, dt.date(2026, 1, 1), dt.date(2026, 10, 5))])
        self.assertEqual(download.in_years([a, b], 2024, 2025),
                         [(a, dt.date(2024, 10, 14), dt.date(2024, 10, 18)),
                          (b, dt.date(2025, 12, 30), dt.date(2025, 12, 31))])
        self.assertEqual(download.in_years([a, b], 2023, 2023), [])


# ---- page ---------------------------------------------------------------------------------------

class PageDownloadTest(PageTestCase):
    def setUp(self):
        super().setUp()
        self.catalogue = self.masts()["masts"]["data"]
        self.server.requests.clear()

    def ticks(self, inputs, changed):
        r = self.call([("download-loggers", "options"), ("download-loggers", "value"), ("download-all", "value")],
                      inputs, state=[("masts", "data", self.catalogue)], changed=changed)
        return r["download-loggers"]["options"], r["download-loggers"]["value"], r["download-all"]["value"]

    def test_layout(self):
        """the download card has year dropdowns, the "All" and logger checklists, the button and a dcc.Download"""
        by_id = {c.id: c for c in app.v2_app.layout._traverse() if getattr(c, "id", None)}
        for cid in ("download-from", "download-to", "download-all", "download-loggers", "download-button",
                    "download-message", "download-file"):
            self.assertIn(cid, by_id)
        self.assertEqual(type(by_id["download-file"]).__name__, "Download")
        self.assertEqual(by_id["download-all"].value, ["all"])

    def test_ticks(self):
        """mast change: all loggers and "All" ticked; unticking a logger unticks "All"; "All" ticks every logger or none"""
        both = [TEKBOX_M6, CAMPBELL_M6]
        opts, value, all_ = self.ticks([("mast", "value", "Mast 6"), ("download-all", "value", []),
                                        ("download-loggers", "value", [])], "mast.value")
        self.assertEqual([o["value"] for o in opts], both)
        self.assertEqual((value, all_), (both, ["all"]))
        _, value, all_ = self.ticks([("mast", "value", "Mast 6"), ("download-all", "value", ["all"]),
                                     ("download-loggers", "value", [CAMPBELL_M6])], "download-loggers.value")
        self.assertEqual((value, all_), ([CAMPBELL_M6], []))
        _, value, all_ = self.ticks([("mast", "value", "Mast 6"), ("download-all", "value", ["all"]),
                                     ("download-loggers", "value", [CAMPBELL_M6])], "download-all.value")
        self.assertEqual((value, all_), (both, ["all"]))
        _, value, all_ = self.ticks([("mast", "value", "Mast 6"), ("download-all", "value", []),
                                     ("download-loggers", "value", both)], "download-all.value")
        self.assertEqual((value, all_), ([], []))
        self.assertEqual(self.server.requests, [])

    def test_years(self):
        """the year dropdowns: first..last year with data of the ticked loggers (Mast 9: 2024-2026; the Campbell only: 2026)"""
        def years(values):
            r = self.call([("download-from", "options"), ("download-from", "value"), ("download-to", "options"),
                           ("download-to", "value")], [("download-loggers", "value", values)],
                          state=[("mast", "value", "Mast 9"), ("masts", "data", self.catalogue)])
            return ([o["value"] for o in r["download-from"]["options"]], r["download-from"]["value"],
                    r["download-to"]["value"])
        self.assertEqual(years([TEKBOX_M9, CAMPBELL_M9]), ([2024, 2025, 2026], 2024, 2026))
        self.assertEqual(years([CAMPBELL_M9]), ([2026], 2026, 2026))
        self.assertEqual(years([]), ([], None, None))

    def download(self, values, start, end, mast="Mast 6"):
        return self.call([("download-file", "data"), ("download-message", "children")],
                         [("download-button", "n_clicks", 1)],
                         state=[("mast", "value", mast), ("download-loggers", "value", values),
                                ("download-from", "value", start), ("download-to", "value", end),
                                ("masts", "data", self.catalogue)])

    def test_callback_send_bytes(self):
        """the button returns dcc.send_bytes data: base64 xlsx named nuki_Mast_6_2026-2026.xlsx, and a message with its size"""
        r = self.download([TEKBOX_M6, CAMPBELL_M6], 2026, 2026)
        data = r["download-file"]["data"]
        self.assertEqual(data["filename"], "nuki_Mast_6_2026-2026.xlsx")
        self.assertTrue(data["base64"])
        self.assertEqual(data["type"], callbacks.XLSX_TYPE)
        wb = open_xlsx(base64.b64decode(data["content"]))
        self.assertEqual(wb.sheetnames, M6_SHEETS)
        self.assertIn("nuki_Mast_6_2026-2026.xlsx", text_of(r["download-message"]["children"]))

    def test_callback_running(self):
        """the download callback disables the button while it runs (running=)"""
        deps = self.http.get(v2.URL_PREFIX + "_dash-dependencies").get_json()
        cb = next(d for d in deps if "download-file.data" in d["output"])
        running = cb.get("running") or {}
        self.assertEqual(running.get("running", {}).get("download-button.disabled"), True)
        self.assertEqual(running.get("runningOff", {}).get("download-button.disabled"), False)

    def test_callback_too_large(self):
        """DOWNLOAD_TIME_BUDGET_S used up (slow stub): no file, the "too large" text next to the button"""
        self.set_settings(download_time_budget_s=0.4)
        self.server.delay = 0.25
        try:
            r = self.download([TEKBOX_M6, CAMPBELL_M6], 2026, 2026)
        finally:
            self.server.delay = 0.0
        self.assertNotIn("download-file", r)
        self.assertIn(texts.DOWNLOAD_TOO_LARGE, text_of(r["download-message"]["children"]))
        self.assertEqual(len(self.api_requests("measurements/raw")), 1)

    def test_callback_no_logger(self):
        """no logger ticked: no file, a warning, no request"""
        r = self.download([], 2026, 2026)
        self.assertNotIn("download-file", r)
        self.assertIn(texts.DOWNLOAD_CHOOSE_LOGGERS, text_of(r["download-message"]["children"]))
        self.assertEqual(self.server.requests, [])


if __name__ == "__main__":
    unittest.main()
