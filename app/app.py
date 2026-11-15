import os
# os.environ["DASH_ASYNC_SUPPORT"] = "0"

from flask import Flask, request
from dash import Dash, Input, Output, State, dcc, html, no_update
import plotly.express as px
import pandas as pd
import dash_ag_grid as dag
import ast
import dash_bootstrap_components as dbc
import datetime
import re
import traceback
from datetime import datetime as dt
import plotly.graph_objects as go

import asyncio

import db_connector

from config import settings
DEBUG = settings.DEBUG
#DEBUG = True

# set up logging to console
import logging
if DEBUG:
    logging.basicConfig(level=logging.DEBUG, force=True)
else:
    logging.basicConfig(level=logging.INFO, force=True)
logger = logging.getLogger(__name__)

logger.info("app.py loaded")

# Print a debug message
logger.info(f"DASH_ASYNC_SUPPORT: {os.environ.get('DASH_ASYNC_SUPPORT', 'Not set!')}")


# Masts and loggers come from the API metadata (db_connector.mast_catalogue), loaded into the
# "catalogue" store when the page loads. The default date range is the last DEFAULT_DAYS days
# of the selected logger's data.
DEFAULT_DAYS = 30


server = Flask(__name__)
app = Dash(__name__,
           server=server,
           requests_pathname_prefix='/app/nuki/',
           routes_pathname_prefix='/app/nuki/')


app.layout = html.Div(
    [
        dcc.Store(id="catalogue"),
        dbc.Row([dbc.Col(html.H1('Structural Health Monitoring Power Line Masts Kangerluarsuk Ungalleq'), width="auto"),
                 # link to the phase 2 page (app/v2/), another Dash app on the same server: plain link
                 dbc.Col(html.A("Try the new page (in development)", id="v2-link", href="/app/nuki/v2/"),
                         width="auto", className="align-self-center")]),
        html.H5("Select the date range (YYYY/MM/DD)"),
        dbc.Row([dbc.Col(html.Div(dcc.DatePickerRange(id='date-range-picker',
                                             min_date_allowed=dt(2023, 1, 1),
                                             max_date_allowed=dt.combine(dt.now().date(), dt.max.time()),
                                             initial_visible_month=dt.now(),
                                             start_date_placeholder_text='Start Date',
                                             end_date_placeholder_text='End Date',
                                             display_format='YYYY/MM/DD',), className='dash-bootstrap'), width=3),
                 ]),
        html.H5("Mast"),
        dcc.Dropdown(id="mast", options=[]),
        html.H5("Logger"),
        dcc.Dropdown(id="logger", options=[]),
        html.Br(),
        dbc.Row(dbc.Col(html.Div(id="graph"))),  # Placeholder for the graph
        dag.AgGrid(  # Placeholder for the table
            id="table",
            columnDefs=[],  # Initialize with no columns
            rowData=[],
            columnSize="sizeToFit",
            defaultColDef={"minWidth": 120, "sortable": True},
            dashGridOptions={"rowSelection": "single"},
        ),
    ],
    className="dbc p-4",
)


# ---- helpers without Dash or HTTP (unit-tested in tests/unit/test_app.py) ---------------------

def error_message(exc, logger_name=""):
    """The message shown in the graphs for an exception of a data request."""
    if isinstance(exc, db_connector.QueryTimeout):
        return "Backend request timed out"
    if isinstance(exc, db_connector.EmptyQuerySet):
        return "No data found for the given query"
    if isinstance(exc, db_connector.RecordingDeviceNotFound):
        return f"Recording device {logger_name} not found"
    if isinstance(exc, db_connector.BackendUnreachable):
        return "Backend not reachable"
    return "An unknown error occurred"


def log_error(message, exc):
    """Log a data error as before: with the traceback for unknown errors."""
    if message == "An unknown error occurred":
        logger.error(f"{message}: {exc}\n{traceback.format_exc()}")
    else:
        logger.error(f"{message}: {exc}")


_LOGGER_LABEL = re.compile(r"^(?P<display>.*) \((?P<ident>[^()]*)\), ")


def logger_short(lg):
    """Short logger name for the titles: "<display name> <serial or deveui>", from the
    catalogue label "<display name> (<serial or deveui>), <first> to <last>"."""
    if not lg:
        return ""
    label = lg.get("label") or lg.get("value") or ""
    m = _LOGGER_LABEL.match(label)
    if m:
        return f"{m.group('display')} {m.group('ident')}"
    return label.split(",", 1)[0]


def find_mast(catalogue, mast_name):
    for mast in (catalogue or {}).get("masts") or []:
        if mast["name"] == mast_name:
            return mast
    return None


def find_logger(catalogue, logger_value):
    """(mast, logger) of a logger value in the catalogue store, or (None, None)."""
    for mast in (catalogue or {}).get("masts") or []:
        for lg in mast["loggers"]:
            if lg["value"] == logger_value:
                return mast, lg
    return None, None


def date_range(lg, days=DEFAULT_DAYS):
    """Date picker settings for a catalogue logger: the last ``days`` days of its data (not
    before its first measurement), and its first and last dates as the limits.
    Returns {"start_date", "end_date", "min_date_allowed", "max_date_allowed"} (YYYY-MM-DD)."""
    first = datetime.date.fromisoformat(lg["first"][:10]) if lg.get("first") else None
    last = datetime.date.fromisoformat(lg["last"][:10])
    start = last - datetime.timedelta(days=days)
    if first is not None and start < first:
        start = first
    return {
        "start_date": start.isoformat(),
        "end_date": last.isoformat(),
        "min_date_allowed": (first or start).isoformat(),
        "max_date_allowed": last.isoformat(),
    }


# (title, y label) of the six graphs, in page order
GRAPHS = [
    ("Battery Level", "Voltage (V)"),
    ("Ground Temperature", "Temperature (°C)"),
    ("Air Temperature", "Temperature (°C)"),
    ("Relative Humidity", "Relative Humidity (%)"),
    ("Barometric Pressure", "Pressure (kPa)"),
    ("Inclination", "Inclination (deg)"),
]


def titled(title, where):
    return f"{title} - {where}" if where else title


def message_figures(message, where=""):
    """The six graphs, each showing the same message."""
    return [plot_no_values(message=message, ylabel=ylabel, title=titled(title, where))
            for title, ylabel in GRAPHS]


def _rows(frame):
    # the rows where the selected series have data, as the v1 per-sensor queries returned them
    # (one logger's frame has the times of all its sensors; the Campbell inclinometer is 6-hourly)
    return frame.dropna(how="all")


def _figure(data, where, title, ylabel, what, select, label=None, legend_title="Measurement"):
    """One graph from a db_connector selection; EmptyQuerySet shows "No data found" in this
    graph only. label: one legend name for every column (default: the selection's labels)."""
    try:
        frame, labels, *unit = select(data)
    except db_connector.EmptyQuerySet as e:
        message = f"No data found for the given {what} query"
        logger.error(f"{message}: {e}")
        return plot_no_values(message=message, ylabel=ylabel, title=titled(title, where))
    if title == "Barometric Pressure":
        ylabel = "Pressure (kPa)" if unit[0] == "kPa" else f"Pressure ({unit[0]})"
    names = [label] * len(frame.columns) if label else labels
    return plot_measures(_rows(frame), ylabel=ylabel, title=titled(title, where),
                         label_names=names, legend_title=legend_title)


def build_figures(data, where):
    """The six figures (GRAPHS order) from a db_connector.LoggerData; ``where`` is the title
    suffix, e.g. "Mast 6 (CR300_M6 12096)"."""
    weather = lambda name: lambda d: db_connector.weather(d, name)  # noqa: E731
    return [
        _figure(data, where, "Battery Level", "Voltage (V)", "BatteryVoltage sensor",
                db_connector.battery, label="Battery level"),
        _figure(data, where, "Ground Temperature", "Temperature (°C)", "ground temperature sensor",
                db_connector.ground_temperature, legend_title="Sensor Depths"),
        _figure(data, where, "Air Temperature", "Temperature (°C)", "weather sensor",
                weather("AirTemp"), label="Air Temperature"),
        _figure(data, where, "Relative Humidity", "Relative Humidity (%)", "weather sensor",
                weather("RelHum"), label="Relative Humidity"),
        _figure(data, where, "Barometric Pressure", "Pressure (kPa)", "weather sensor",
                weather("BarometricPressure"), label="Barometric Pressure"),
        _figure(data, where, "Inclination", "Inclination (deg)", "inclination sensor",
                db_connector.inclination),
    ]


def ground_temperature_table(data):
    """(rowData, columnDefs) of the ground temperature table: one column per sensor (sensor
    name, e.g. GroundTemp01), shallow to deep; no timestamps (as before; fixed in phase 2).
    Empty when the logger has no ground temperatures."""
    try:
        frame, _labels = db_connector.ground_temperature(data)
    except db_connector.EmptyQuerySet:
        return [], []
    frame = _rows(frame).rename(columns={k: data.columns[k].get("sensor_name") or k for k in frame.columns})
    frame = frame.loc[:, ~frame.columns.duplicated()]
    return frame.to_dict("records"), [{"field": c} for c in frame.columns]


def layout_graphs(figs):
    graphs = [dcc.Graph(figure=fig, className="border") for fig in figs]
    rows = [dbc.Row([dbc.Col(g, lg=6),]) for g in graphs[:-1]]
    rows.append(dbc.Row([dbc.Col(graphs[-1], lg=6),], className="mt-4"))
    return rows


# ---- callbacks --------------------------------------------------------------------------------
# catalogue (page load) -> mast options and value -> logger options and value -> date range
# -> data (one request set per update)

@app.callback(
    [Output("catalogue", "data"), Output("mast", "options"), Output("mast", "value")],
    Input("mast", "id"),  # This input is just a placeholder to trigger the callback at page load
)
async def load_catalogue(_):
    """The mast catalogue into the store; the first mast is selected."""
    try:
        masts = await db_connector.mast_catalogue_async()
    except Exception as e:
        message = error_message(e)
        log_error(message, e)
        return {"masts": [], "error": f"Mast list not available: {message}"}, [], None
    options = [{"label": m["name"], "value": m["name"]} for m in masts]
    if not masts:
        return {"masts": [], "error": "No masts found"}, [], None
    return {"masts": masts, "error": None}, options, masts[0]["name"]


@app.callback(
    [Output("logger", "options"), Output("logger", "value")],
    Input("mast", "value"),
    State("catalogue", "data"),
)
def update_loggers(mast_name, catalogue):
    """The loggers of the selected mast; the default is the one with the latest data."""
    mast = find_mast(catalogue, mast_name)
    if mast is None:
        return [], None
    options = [{"label": lg["label"], "value": lg["value"]} for lg in mast["loggers"]]
    default = db_connector.default_logger(mast)
    return options, (default["value"] if default else None)


@app.callback(
    [Output("date-range-picker", "start_date"), Output("date-range-picker", "end_date"),
     Output("date-range-picker", "min_date_allowed"), Output("date-range-picker", "max_date_allowed"),
     Output("date-range-picker", "initial_visible_month")],
    Input("logger", "value"),
    State("catalogue", "data"),
)
def update_date_range(logger_value, catalogue):
    """The last 30 days of the selected logger's data, limited to its first..last dates."""
    _mast, lg = find_logger(catalogue, logger_value)
    if lg is None:
        return no_update, no_update, no_update, no_update, no_update
    r = date_range(lg)
    return r["start_date"], r["end_date"], r["min_date_allowed"], r["max_date_allowed"], r["end_date"]


@app.callback(
    [Output("graph", "children"), Output("table", "rowData"), Output("table", "columnDefs")],
    [Input("logger", "value"), Input("date-range-picker", "start_date"), Input("date-range-picker", "end_date"),
     Input("catalogue", "data")],
)
async def get_data_from_measurement(logger_value, start_date, end_date, catalogue):
    """Get data from the database and plot it"""

    # How do I get the IP address of the client sending the request?
    client_ip = request.remote_addr
    request_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.debug(f"{request_time} - {client_ip} - Requesting # Logger: {logger_value}; Start Date: {start_date}; End Date: {end_date}")

    mast, lg = find_logger(catalogue, logger_value)
    if lg is None:
        message = (catalogue or {}).get("error") or "Select a mast and logger"
        return layout_graphs(message_figures(message)), [], []

    short = logger_short(lg)
    where = f"{mast['name']} ({short})"
    try:
        data = await db_connector.get_logger_data_async(logger_value, start_date, end_date)
    except Exception as e:
        message = error_message(e, short)
        log_error(message, e)
        return layout_graphs(message_figures(message, where)), [], []

    logging.debug(data.frame)
    row_data, column_defs = ground_temperature_table(data)
    return layout_graphs(build_figures(data, where)), row_data, column_defs


def plot_measures(df, ylabel="", title="", label_names=None, legend_title=""):
    """General plotting function
    Takes a dataframe and plots each column as a line on the same graph
    """

    fig = go.Figure()

    for id, column in enumerate(df.columns):

        if label_names is not None:
            label = label_names[id]
        else:
            label = column

        fig.add_trace(go.Scatter(
            x=df.index,  # X-axis (Timestamp)
            y=df[column],  # Y-axis (Temperature values)
            mode='lines+markers',  # Lines and markers
            name=label,  # Label for each line (Depth sensor)
            marker=dict(size=6),  # Marker customization
        ))

    fig.update_layout(
        title=title,
        xaxis_title='Time',
        yaxis_title=ylabel,
        legend_title=legend_title,
        template='plotly_white'
    )

    return fig


def plot_no_values(message="No data returned...", ylabel="", title=""):
    """Plot a message when no values are available"""

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=[0],  # X-axis (Timestamp)
        y=[0],  # Y-axis (Temperature values)
        mode='text',  # Text mode
        text=message,  # Text message
        textfont=dict(size=24, color='grey', family='Arial', weight=100)  # Font size, color, and bold text
    ))

    fig.update_layout(
        title=title,
        xaxis_title='Time',
        yaxis_title=ylabel,
        template='plotly_white'
    )

    return fig


# the phase 2 page (app/v2/) at /app/nuki/v2/, a second Dash app on the same Flask server
import v2  # noqa: E402
v2_app = v2.create_app(server)


if __name__ == "__main__":
    #app.run()
    app.run_server(debug=DEBUG)
