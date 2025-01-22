from flask import Flask
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
                                             max_date_allowed=dt.now(),
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

#Still have to implement if the user want to change the date range, without changing the mast.
@callback(
    [Output("graph", "children"), Output("table", "rowData"), Output("table", "columnDefs")],
    [Input("mast", "value"), Input("date-range-picker", "start_date"),Input("date-range-picker", "end_date")], 
)

def get_data_from_measurement(mast, start_date, end_date):
    logger.info(f"Selected mast: {mast} (info)")
    logger.debug(f"Selected mast: {mast} (debug)")
    if mast is None:
        mast = mastopt[0]

    logger.debug(f"Selected mast: {mast}")
    logger.debug(f"Selected start date: {start_date}")
    logger.debug(f"Selected end date: {end_date}")

    start = start_date
    stop = end_date
    
    #Based on selection of mast in app, the associated dev_eui is selected below
    dev_eui = mast_dict[mast]

    df_gt, sensor_depth_dict, gt_dt_obj = db_connector.get_ground_temp(dev_eui, start, stop)
    df_weather = db_connector.get_sensor_data(dev_eui, "Weather Sensor", start, stop)
    df_incl = db_connector.get_sensor_data(dev_eui, "Inclination Sensor", start, stop)

    fig_gt = plot_ground_temp(df_gt, sensor_depth_dict, gt_dt_obj, mast)
    fig_airtemp = plot_airtemp(df_weather[['AirTemp']], mast)   # duble brackets to keep it as a dataframe
    fig_rh = plot_rh(df_weather[['RelHum']], mast)              # duble brackets to keep it as a dataframe
    fig_bp = plot_pressure(df_weather[['BarometricPressure']], mast)  # duble brackets to keep it as a dataframe
    fig_incl = plot_inclination(df_incl, mast)  

    graph1 = dcc.Graph(figure=fig_gt, className="border")
    graph2 = dcc.Graph(figure=fig_airtemp, className="border")
    graph3 = dcc.Graph(figure=fig_rh, className="border")
    graph4 = dcc.Graph(figure=fig_bp, className="border")
    graph5 = dcc.Graph(figure=fig_incl, className="border")

    all_graphs = [
        dbc.Row([dbc.Col(graph1, lg=6), dbc.Col(graph2, lg=6)]),
        dbc.Row([dbc.Col(graph3, lg=6), dbc.Col(graph4, lg=6)]),
        dbc.Row([dbc.Col(graph5, lg=6)], className="mt-4"),
    ]

    # Prepare data for the table
    row_data = df_gt.to_dict("records")
    column_defs = [{"field": i} for i in df_gt.columns]

    return all_graphs, row_data, column_defs
 

def plot_ground_temp(df, sensor_depth_dict, datatype_obj, mast):
    fig_gt = go.Figure()
    #logger.debug(ground_temperatures.columns)
    for column in df.columns:
            fig_gt.add_trace(go.Scatter(
                x=df.index,  # X-axis (Timestamp)
                y=df[column],  # Y-axis (Temperature values)
                mode='lines+markers',  # Lines and markers
                name=f"{sensor_depth_dict[column]:+0.2f} m",  # Label for each line (Depth sensor)
                marker=dict(size=6)  # Marker customization
            ))

    # Function should be updated to obtain this information
    # dynamically from the database...
    fig_gt.update_layout(
            title=f"Ground Temperature - {mast}",
            xaxis_title='Time',
            yaxis_title='Temperature (°C)',
            legend_title='Sensor Depth',
            template='plotly_white'
        )

    return fig_gt

# These plotting functions could be generalized to accept a list of columns
# and a list of names to be used as labels for each column
# This would significantly reduce the amount of code needed to plot

def plot_airtemp(df, mast):
    fig_airtemp = go.Figure()

    fig_airtemp.add_trace(go.Scatter(
                x=df.index,  # X-axis (Timestamp)
                y=df["AirTemp"],  # Y-axis (Temperature values)
                mode='lines+markers',  # Lines and markers
                name="Air Temperature", 
                marker=dict(size=6)  # Marker customization
            ))

    fig_airtemp.update_layout(
        xaxis_title='Time',
        yaxis_title='Air Temperature (°C)',
        showlegend=False,
        template='plotly_white'
    )

    return fig_airtemp

def plot_rh(df, mast):
    fig_rh = go.Figure()

    fig_rh.add_trace(go.Scatter(
            x=df.index,  # X-axis (Timestamp)
            y=df["RelHum"],  # Y-axis 
            mode='lines+markers',  # Lines and markers
            name="Relative Humidity",  # Label for each line (Depth sensor)
            marker=dict(size=6)  # Marker customization
        ))

    fig_rh.update_layout(
        xaxis_title='Time',
        yaxis_title='Relative Humidity (%)',
        showlegend=False,
        template='plotly_white'
    )

    return fig_rh


def plot_pressure(df, mast):
    fig_bp = go.Figure()

    fig_bp.add_trace(go.Scatter(
                x=df.index,  # X-axis (Timestamp)
                y=df["BarometricPressure"],  # Y-axis (Temperature values)
                mode='lines+markers',  # Lines and markers
                name="Barometric Pressure",  # Label for each line (Depth sensor)
                marker=dict(size=6)  # Marker customization
            ))
    
    fig_bp.update_layout(
        xaxis_title='Time',
        yaxis_title='Barometric Pressure (kPa)',
        showlegend=False,
        template='plotly_white'
    )

    return fig_bp

def plot_inclination(df, mast):

    fig_incl = go.Figure()
    
    for column in df.columns:
        fig_incl.add_trace(go.Scatter(
            x=df.index,  # X-axis (Timestamp)
            y=df[column],  # Y-axis (Temperature values)
            mode='lines+markers',  # Lines and markers
            name=column,  # Label for each line (Depth sensor)
            marker=dict(size=6)  # Marker customization
        ))

    fig_incl.update_layout(
        xaxis_title='Time',
        yaxis_title='Inclination (deg)',
        legend_title='Measurement',
        template='plotly_white'
    )

    return fig_incl


if __name__ == "__main__":
    #app.run()
    app.run_server(debug=DEBUG)
