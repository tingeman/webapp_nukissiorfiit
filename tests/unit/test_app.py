"""Unit tests of the phase 1 page (app/app.py) without a browser: the helpers that build the
date range, titles, figures and table, and the callbacks called as functions against the
stub monitoring_db API (tests/fake_api.py, data described in tests/fixtures/make_fixtures.py).
Each test's docstring is its name in the test output ("unit: app: <docstring>")."""
import asyncio
import logging

import pandas as pd

from support import ApiTestCase, db_connector, fake_api

import app  # noqa: E402  (app/app.py, after support has put /app on sys.path)

TEKBOX_M6 = "deveui:feedbeefcafe0002"          # device 10, 2026-09-01..05
CAMPBELL_M6 = "serial:SIS2024-01:12096"        # device 11, 2026-10-01..05
TITLES = ["Battery Level", "Ground Temperature", "Air Temperature", "Relative Humidity",
          "Barometric Pressure", "Inclination"]


def synthetic(columns, n=3):
    """LoggerData without HTTP: columns is {key: details}; values 1, 2, 3 ... per column."""
    index = pd.date_range("2026-10-01", periods=n, freq="3h", tz="UTC", name="timestamp")
    frame = pd.DataFrame({k: [float(i + 1) for i in range(n)] for k in columns}, index=index)
    return db_connector.LoggerData(frame=frame, columns={k: dict(v, key=k) for k, v in columns.items()})


def title(fig):
    return fig.layout.title.text


def texts(fig):
    return [t.text for t in fig.data]


def figs_of(graphs):
    """The figures of the callback's graph rows (Row > Col > Graph)."""
    return [row.children[0].children.figure for row in graphs]


class HelperTest(ApiTestCase):
    def test_logger_short(self):
        """logger_short() is "<display name> <serial or deveui>" from the catalogue label"""
        self.assertEqual(app.logger_short({"value": "x", "label": "CR300_M6 (12096), 2026-10-01 to 2026-10-05"}),
                         "CR300_M6 12096")
        self.assertEqual(app.logger_short({"value": "x", "label": "odd label, rest"}), "odd label")
        self.assertEqual(app.logger_short(None), "")

    def test_date_range(self):
        """date_range() is the last 30 days of a logger's data, limited to its first..last dates"""
        lg = {"first": "2024-09-11T06:00:00Z", "last": "2026-09-13T21:00:00Z"}
        self.assertEqual(app.date_range(lg), {"start_date": "2026-08-14", "end_date": "2026-09-13",
                                              "min_date_allowed": "2024-09-11", "max_date_allowed": "2026-09-13"})
        lg = {"first": "2026-09-01T00:00:00Z", "last": "2026-09-05T00:00:00Z"}
        self.assertEqual(app.date_range(lg), {"start_date": "2026-09-01", "end_date": "2026-09-05",
                                              "min_date_allowed": "2026-09-01", "max_date_allowed": "2026-09-05"})

    def test_error_message(self):
        """error_message() gives the page's messages for timeout, no data, logger not found, unreachable backend and other errors"""
        self.assertEqual(app.error_message(db_connector.QueryTimeout("x")), "Backend request timed out")
        self.assertEqual(app.error_message(db_connector.EmptyQuerySet("x")), "No data found for the given query")
        self.assertEqual(app.error_message(db_connector.RecordingDeviceNotFound("x"), "CR300_M6 12096"),
                         "Recording device CR300_M6 12096 not found")
        self.assertEqual(app.error_message(db_connector.BackendUnreachable("x")), "Backend not reachable")
        self.assertEqual(app.error_message(RuntimeError("x")), "An unknown error occurred")

    def test_figures_tekbox(self):
        """build_figures() for a Tekbox: six titled figures, depth labels, pressure in kPa, Roll/Pitch only"""
        data = db_connector.get_logger_data(TEKBOX_M6, "2026-09-01", "2026-09-01")
        figs = app.build_figures(data, "Mast 6 (Tekbox_TBSL1_3 feedbeefcafe0002)")
        self.assertEqual([title(f) for f in figs],
                         [f"{t} - Mast 6 (Tekbox_TBSL1_3 feedbeefcafe0002)" for t in TITLES])
        self.assertEqual([t.name for t in figs[0].data], ["Battery level"])
        self.assertEqual([t.name for t in figs[1].data], ["-0.10 m", "-0.25 m", "-0.50 m"])
        self.assertEqual(figs[4].layout.yaxis.title.text, "Pressure (kPa)")
        self.assertAlmostEqual(list(figs[4].data[0].y)[0], 101.3)
        self.assertEqual([t.name for t in figs[5].data], ["PitchAngle", "RollAngle"])
        self.assertEqual(figs[5].layout.yaxis.title.text, "Inclination (deg)")
        self.assertEqual(len(figs[5].data[0].y), 8)

    def test_figures_campbell(self):
        """build_figures() for a Campbell: X/Y angles, plotted only at the inclinometer's own (6-hourly) times"""
        data = db_connector.get_logger_data(CAMPBELL_M6, "2026-10-01", "2026-10-01")
        figs = app.build_figures(data, "Mast 6 (CR300_M6 12096)")
        self.assertEqual([t.name for t in figs[5].data], ["XAngle", "YAngle"])
        self.assertEqual([round(y, 6) for y in figs[5].data[0].y], [0.45, 0.452, 0.454, 0.456])
        self.assertEqual(len(figs[0].data[0].y), 8)

    def test_figures_missing_selection(self):
        """build_figures(): a selection without series shows "No data found" in its own graph only"""
        data = synthetic({
            "BatteryVoltage__BatteryVoltage": {"timeseries_name": "BatteryVoltage", "sensor_name": "BatteryVoltage"},
            "W__BarometricPressure": {"timeseries_name": "BarometricPressure", "unit": "hPa"},
        })
        with self.assertLogs("app", level="ERROR"):
            figs = app.build_figures(data, "Mast 6 (x)")
        self.assertEqual([t.name for t in figs[0].data], ["Battery level"])
        self.assertEqual(texts(figs[1]), ["No data found for the given ground temperature sensor query"])
        self.assertEqual(texts(figs[2]), ["No data found for the given weather sensor query"])
        self.assertEqual(texts(figs[5]), ["No data found for the given inclination sensor query"])
        self.assertEqual(figs[4].layout.yaxis.title.text, "Pressure (kPa)")
        self.assertEqual([round(y, 6) for y in figs[4].data[0].y], [0.1, 0.2, 0.3])

    def test_pressure_unknown_unit(self):
        """build_figures(): pressure in an unknown unit is labelled with that unit"""
        data = synthetic({"W__BarometricPressure": {"timeseries_name": "BarometricPressure", "unit": "mbar"}})
        with self.assertLogs("db_connector", level="WARNING"), self.assertLogs("app", level="ERROR"):
            figs = app.build_figures(data, "")
        self.assertEqual(figs[4].layout.yaxis.title.text, "Pressure (mbar)")
        self.assertEqual(title(figs[4]), "Barometric Pressure")

    def test_table(self):
        """ground_temperature_table(): one column per ground sensor (sensor names), one row per time"""
        data = db_connector.get_logger_data(CAMPBELL_M6, "2026-10-01", "2026-10-01")
        rows, cols = app.ground_temperature_table(data)
        self.assertEqual(cols, [{"field": "GroundTemp01"}, {"field": "GroundTemp02"}, {"field": "GroundTemp03"}])
        self.assertEqual(len(rows), 8)
        self.assertAlmostEqual(rows[0]["GroundTemp01"], -1.1)
        self.assertEqual(app.ground_temperature_table(synthetic({"a": {"timeseries_name": "X"}})), ([], []))


class CallbackTest(ApiTestCase):
    def setUp(self):
        super().setUp()
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def catalogue(self):
        return {"masts": db_connector.mast_catalogue(), "error": None}

    def run_data(self, *args):
        with app.server.test_request_context("/app/nuki/_dash-update-component", method="POST"):
            return asyncio.run(app.get_data_from_measurement(*args))

    def test_load_catalogue(self):
        """load_catalogue() stores the catalogue, the masts as options and selects the first"""
        store, options, value = asyncio.run(app.load_catalogue("mast"))
        self.assertIsNone(store["error"])
        self.assertEqual(options, [{"label": n, "value": n} for n in ("Mast 6", "Mast 9", "Mast 13")])
        self.assertEqual(value, "Mast 6")

    def test_load_catalogue_error(self):
        """load_catalogue(): an API failure leaves the dropdown empty and the data callback shows the message"""
        self.server.force = fake_api.QUERY_TIMEOUT
        store, options, value = asyncio.run(app.load_catalogue("mast"))
        self.assertEqual((options, value), ([], None))
        self.assertEqual(store["error"], "Mast list not available: Backend request timed out")
        self.assertEqual(app.update_loggers(value, store), ([], None))
        graphs, rows, cols = self.run_data(None, None, None, store)
        self.assertEqual([texts(f) for f in figs_of(graphs)], [["Mast list not available: Backend request timed out"]] * 6)
        self.assertEqual((rows, cols), ([], []))

    def test_loggers_and_dates(self):
        """update_loggers() defaults to the latest logger; update_date_range() follows the logger"""
        catalogue = self.catalogue()
        options, value = app.update_loggers("Mast 6", catalogue)
        self.assertEqual(sorted(o["value"] for o in options), sorted([TEKBOX_M6, CAMPBELL_M6]))
        self.assertEqual(value, CAMPBELL_M6)
        self.assertEqual(app.update_date_range(TEKBOX_M6, catalogue),
                         ("2026-09-01", "2026-09-05", "2026-09-01", "2026-09-05", "2026-09-05"))
        self.assertTrue(all(v is app.no_update for v in app.update_date_range(None, catalogue)))

    def test_data_one_request_set(self):
        """get_data_from_measurement() makes one /raw request for a 5-day range and returns 6 graphs and the table"""
        graphs, rows, cols = self.run_data(TEKBOX_M6, "2026-09-01", "2026-09-05", self.catalogue())
        self.assertEqual(len(self.api_requests("measurements/raw")), 1)
        self.assertEqual([title(f) for f in figs_of(graphs)],
                         [f"{t} - Mast 6 (Tekbox_TBSL1_3 feedbeefcafe0002)" for t in TITLES])
        self.assertEqual((len(rows), len(cols)), (33, 3))

    def test_data_errors(self):
        """get_data_from_measurement(): a fetch error shows its message in all six graphs, without a table"""
        catalogue = self.catalogue()
        for force, message in ((fake_api.QUERY_TIMEOUT, "Backend request timed out"),
                               ((404, {"error": {"code": "DEVEUI_NOT_FOUND", "message": "x", "details": {}}}),
                                "Recording device Tekbox_TBSL1_3 feedbeefcafe0002 not found"),
                               ((500, {"error": {"code": "X", "message": "x", "details": {}}}),
                                "An unknown error occurred")):
            self.server.force = force
            with self.subTest(message=message):
                graphs, rows, cols = self.run_data(TEKBOX_M6, "2026-09-01", "2026-09-05", catalogue)
                self.assertEqual([texts(f) for f in figs_of(graphs)], [[message]] * 6)
                self.assertEqual((rows, cols), ([], []))
        self.server.force = None
        graphs, _rows, _cols = self.run_data(TEKBOX_M6, "2025-01-01", "2025-01-02", catalogue)
        self.assertEqual(texts(figs_of(graphs)[0]), ["No data found for the given query"])

    def test_no_logger(self):
        """get_data_from_measurement() without a logger asks for a selection and makes no request"""
        catalogue = self.catalogue()
        self.server.requests.clear()
        graphs, rows, cols = self.run_data(None, None, None, catalogue)
        self.assertEqual([texts(f) for f in figs_of(graphs)], [["Select a mast and logger"]] * 6)
        self.assertEqual((rows, cols, self.server.requests), ([], [], []))
