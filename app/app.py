import os
# os.environ["DASH_ASYNC_SUPPORT"] = "0"

from flask import Flask, request
from dash import Dash, Input, Output, callback, dcc, html
import plotly.express as px
import pandas as pd
from influxdb_client import InfluxDBClient
import dash_ag_grid as dag
import ast
import dash_bootstrap_components as dbc
import datetime
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go

import asyncio

import db_connector

import influx_test
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



mastopt = ["Mast 6", "Mast 7", "Mast 9", "Mast 12", "Mast 13"]

#Identify the requested mast

# rename to device_eui
codenamesformasts = ['feedbeefcafe0002',     #Mast6 has position 1 in list with options
                    'feedbeefcafe0003',     #Mast7 has position 2 in list with options
                    'feedbeefcafe0001',    #Mast9 has position 3 in list with options
                    'feedbeefcafe0004',     #Mast12 has position 4 in list with options
                    'feedbeefcafe0005']     #Mast13

mast_dict = {'Mast 6': 'feedbeefcafe0002',
             'Mast 7': 'feedbeefcafe0003',
             'Mast 9': 'feedbeefcafe0001',
             'Mast 12': 'feedbeefcafe0004',
             'Mast 13': 'feedbeefcafe0005'}

end_date = datetime.date.today()
start_date = end_date - relativedelta(months=1)


server = Flask(__name__)
app = Dash(__name__,                
           server=server,
           requests_pathname_prefix='/app/nuki/',
           routes_pathname_prefix='/app/nuki/')


app.layout = html.Div(
    [
        dbc.Row(dbc.Col(html.H1('Structural Health Monitoring Power Line Masts Kangerluarsuk Ungalleq'), width="auto")),
        html.H5("Select the date range (YYYY/MM/DD)"),
        dbc.Row([dbc.Col(html.Div(dcc.DatePickerRange(id='date-range-picker',
                                             min_date_allowed=dt(2023, 1, 1),
                                             max_date_allowed=dt.combine(dt.now().date(), dt.max.time()),
                                             initial_visible_month=dt.now(),
                                             start_date_placeholder_text='Start Date',
                                             end_date_placeholder_text='End Date',
                                             start_date=start_date,
                                             end_date=end_date,
                                             persistence=True,
                                             persistence_type='session',
                                             display_format='YYYY/MM/DD',), className='dash-bootstrap'), width=3),
                 ]),
        html.H5("Mast"),
        dcc.Dropdown(
            id="mast", options=["Mast 6", "Mast 7", "Mast 9", "Mast 12", "Mast 13"]
        ),
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


@app.callback(
    Output('date-range-picker', 'max_date_allowed'),
    Input('date-range-picker', 'id')  # This input is just a placeholder to trigger the callback
)
def update_max_date_allowed(_):
    return dt.combine(dt.now().date(), dt.max.time())


#Still have to implement if the user want to change the date range, without changing the mast.
@callback(
    [Output("graph", "children"), Output("table", "rowData"), Output("table", "columnDefs")],
    [Input("mast", "value"), Input("date-range-picker", "start_date"),Input("date-range-picker", "end_date")], 
)

async def get_data_from_measurement(mast, start_date, end_date):
    """Get data from the database and plot it"""

    # How do I get the IP address of the client sending the request?
    client_ip = request.remote_addr
    request_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.debug(f"{request_time} - {client_ip} - Requesting # Mast: {mast}; Start Date: {start_date}; End Date: {end_date}")

    if mast is None:
        mast = mastopt[0]

    start = start_date
    stop = end_date
    
    #Based on selection of mast in app, the associated dev_eui is selected below
    dev_eui = mast_dict[mast]

    ylabel = "Voltage (V)"
    title = f"Battery Level - {mast}"
    import traceback
    try:
        df_batlev = await db_connector.get_sensor_data_async(dev_eui, "BatteryVoltage", start, stop)
        fig_batlev = plot_measures(df_batlev, ylabel=ylabel, title=title, label_names="Battery level", legend_title="Measurement")
    except db_connector.QueryTimeout as e:
        message = "Backend request timed out for BatteryVoltage sensor query"
        logger.error(f"{message}: {e}")
        df_batlev = None
        fig_batlev = plot_no_values(message=message, ylabel=ylabel, title=title)
    except db_connector.EmptyQuerySet as e:
        message = "No data found for the given BatteryVoltage sensor query"
        logger.error(f"{message}: {e}")
        df_batlev = None
        fig_batlev = plot_no_values(message=message, ylabel=ylabel, title=title)
    except db_connector.RecordingDeviceNotFound as e:
        message = f"Recording device {dev_eui} not found"
        logger.error(f"{message}: {e}")
        df_batlev = None
        fig_batlev = plot_no_values(message=message, ylabel=ylabel, title=title)
    except Exception as e:
        message = "An unknown error occurred"
        logger.error(f"{message}: {e}\n{traceback.format_exc()}")
        df_batlev = None
        fig_batlev = plot_no_values(message=message, ylabel=ylabel, title=title)


    logging.debug(df_batlev)
    

    ylabel = "Temperature (°C)"
    title = f"Ground Temperature - {mast}"
    try:
        df_gt, gt_dt_obj = await db_connector.get_ground_temp_async(dev_eui, start, stop)
        sensor_depth_dict = await db_connector.get_ground_temp_sensor_depths_async(dev_eui)
        fig_gt = plot_measures(df_gt, ylabel=ylabel, title=title, label_names=[f"{sensor_depth_dict[column]:+0.2f} m" for column in df_gt.columns], legend_title="Sensor Depths")
    except db_connector.QueryTimeout as e:
        message = "Backend request timed out for ground temperature sensor query"
        logger.error(f"{message}: {e}")
        df_gt = None
        fig_gt = plot_no_values(message=message, ylabel=ylabel, title=title)
    except db_connector.EmptyQuerySet as e:
        message = "No data found for the given ground temperature sensor query"
        logger.error(f"{message}: {e}")
        df_gt = None
        fig_gt = plot_no_values(message=message, ylabel=ylabel, title=title)
    except db_connector.RecordingDeviceNotFound as e:
        message = f"Recording device {dev_eui} not found"
        logger.error(f"{message}: {e}")
        df_gt = None
        fig_gt = plot_no_values(message=message, ylabel=ylabel, title=title)
    except Exception as e:
        message = "An unknown error occurred"
        logger.error(f"{message}: {e}")
        df_gt = None
        fig_gt = plot_no_values(message=message, ylabel=ylabel, title=title)


    try:
        df_weather = await db_connector.get_sensor_data_async(dev_eui, "Weather Sensor", start, stop)
        fig_airtemp = plot_measures(df_weather[['AirTemp']], ylabel="Temperature (°C)", title=f"Air Temperature - {mast}", label_names=["Air Temperature"], legend_title="Measurement")
        fig_rh = plot_measures(df_weather[['RelHum']], ylabel="Relative Humidity (%)", title=f"Relative Humidity - {mast}", label_names=["Relative Humidity"], legend_title="Measurement")
        fig_bp = plot_measures(df_weather[['BarometricPressure']], ylabel="Pressure (kPa)", title=f"Barometric Pressure - {mast}", label_names=["Barometric Pressure"], legend_title="Measurement")
    except db_connector.QueryTimeout as e:
        message = "Backend request timed out for weather sensor query"
        logger.error(f"{message}: {e}")
        df_weather = None
        fig_airtemp = plot_no_values(message=message, ylabel="Temperature (°C)", title=f"Air Temperature - {mast}")
        fig_rh = plot_no_values(message=message, ylabel="Relative Humidity (%)", title=f"Relative Humidity - {mast}")
        fig_bp = plot_no_values(message=message, ylabel="Pressure (kPa)", title=f"Barometric Pressure - {mast}")
    except db_connector.EmptyQuerySet as e:
        message = "No data found for the given weather sensor query"
        logger.error(f"{message}: {e}")
        df_weather = None
        fig_airtemp = plot_no_values(message=message, ylabel="Temperature (°C)", title=f"Air Temperature - {mast}")
        fig_rh = plot_no_values(message=message, ylabel="Relative Humidity (%)", title=f"Relative Humidity - {mast}")
        fig_bp = plot_no_values(message=message, ylabel="Pressure (kPa)", title=f"Barometric Pressure - {mast}")
    except db_connector.RecordingDeviceNotFound as e:
        message = f"Recording device {dev_eui} not found"
        logger.error(f"{message}: {e}")
        df_weather = None
        fig_airtemp = plot_no_values(message=message, ylabel="Temperature (°C)", title=f"Air Temperature - {mast}")
        fig_rh = plot_no_values(message=message, ylabel="Relative Humidity (%)", title=f"Relative Humidity - {mast}")
        fig_bp = plot_no_values(message=message, ylabel="Pressure (kPa)", title=f"Barometric Pressure - {mast}")
    except Exception as e:
        message = "An unknown error occurred"
        logger.error(f"{message}: {e}")
        df_weather = None
        fig_airtemp = plot_no_values(message=message, ylabel="Temperature (°C)", title=f"Air Temperature - {mast}")
        fig_rh = plot_no_values(message=message, ylabel="Relative Humidity (%)", title=f"Relative Humidity - {mast}")
        fig_bp = plot_no_values(message=message, ylabel="Pressure (kPa)", title=f"Barometric Pressure - {mast}")


    try:
        df_incl = await db_connector.get_sensor_data_async(dev_eui, "Inclination Sensor", start, stop)
        fig_incl = plot_measures(df_incl, ylabel="Inclination (deg)", title=f"Inclination - {mast}", legend_title="Measurement")
    except db_connector.QueryTimeout as e:
        message = "Backend request timed out for inclination sensor query"
        logger.error(f"{message}: {e}")
        df_incl = None
        fig_incl = plot_no_values(message=message, ylabel="Inclination (deg)", title=f"Inclination - {mast}")
    except db_connector.EmptyQuerySet as e:
        message = "No data found for the given inclination sensor query"
        logger.error(f"{message}: {e}")
        df_incl = None
        fig_incl = plot_no_values(message=message, ylabel="Inclination (deg)", title=f"Inclination - {mast}")
    except db_connector.RecordingDeviceNotFound as e:
        message = f"Recording device {dev_eui} not found"
        logger.error(f"{message}: {e}")
        df_incl = None
        fig_incl = plot_no_values(message=message, ylabel="Inclination (deg)", title=f"Inclination - {mast}")
    except Exception as e:
        message = "An unknown error occurred"
        logger.error(f"{message}: {e}")
        df_incl = None
        fig_incl = plot_no_values(message=message, ylabel="Inclination (deg)", title=f"Inclination - {mast}")


    graph0 = dcc.Graph(figure=fig_batlev, className="border")
    graph1 = dcc.Graph(figure=fig_gt, className="border")
    graph2 = dcc.Graph(figure=fig_airtemp, className="border")
    graph3 = dcc.Graph(figure=fig_rh, className="border")
    graph4 = dcc.Graph(figure=fig_bp, className="border")
    graph5 = dcc.Graph(figure=fig_incl, className="border")

    all_graphs = [
        dbc.Row([dbc.Col(graph0, lg=6),]),
        dbc.Row([dbc.Col(graph1, lg=6),]),
        dbc.Row([dbc.Col(graph2, lg=6),]),
        dbc.Row([dbc.Col(graph3, lg=6),]),
        dbc.Row([dbc.Col(graph4, lg=6),]),
        dbc.Row([dbc.Col(graph5, lg=6),], className="mt-4"),
        # dbc.Row([dbc.Col(graph1, lg=6), dbc.Col(graph2, lg=6)]),
        # dbc.Row([dbc.Col(graph3, lg=6), dbc.Col(graph4, lg=6)]),
        # dbc.Row([dbc.Col(graph5, lg=6)], className="mt-4"),
    ]

    # Prepare data for the table
    if df_gt is not None:
        row_data = df_gt.to_dict("records")
        column_defs = [{"field": i} for i in df_gt.columns]
    else:
        row_data = []
        column_defs = []


    return all_graphs, row_data, column_defs
 

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


if __name__ == "__main__":
    #app.run()
    app.run_server(debug=DEBUG)
