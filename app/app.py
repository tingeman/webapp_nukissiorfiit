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

    
    measurement_names_gt = ["device_frmpayload_data_GroundTemp01",
                                "device_frmpayload_data_GroundTemp03",
                                "device_frmpayload_data_GroundTemp04",
                                "device_frmpayload_data_GroundTemp05",
                                "device_frmpayload_data_GroundTemp06",
                                "device_frmpayload_data_GroundTemp07",
                                "device_frmpayload_data_GroundTemp08",
                                "device_frmpayload_data_GroundTemp09",
                                "device_frmpayload_data_GroundTemp10",
                                "device_frmpayload_data_GroundTemp11",
                                "device_frmpayload_data_GroundTemp12"]
  
    #measurement_names_weather = ["device_frmpayload_data_RelHum",
    #                            "device_frmpayload_data_AirTemp",
    #                            "device_frmpayload_data_DewFrostPoint",
    #                            "device_frmpayload_data_BarometricPressure",
    #                            "device_frmpayload_data_AbsHum",
    #                            "device_frmpayload_data_CloudBase",
    #                            "device_frmpayload_data_Altitude"]

    #measurement_names_incl =    ["device_frmpayload_data_RollAngle",
    #                            "device_frmpayload_data_PitchAngle",
    #                           "device_frmpayload_data_CompassHeading"]
    
    #timestamp_measurement = "device_frmpayload_data_Timestamp"
    bucket = "data_bucket"
                        
    df_gt = influx_test.get_measurement_from_influxdb(bucket, dev_eui, measurement_names_gt, start, stop)
    #df_incl = influx_test.get_measurement_from_influxdb(bucket, dev_eui, measurement_names_incl, start, stop)
    #df_airtemp = influx_test.get_measurement_from_influxdb(bucket, dev_eui, measurement_names_weather[1], start, stop)
    #df_rh = influx_test.get_measurement_from_influxdb(bucket, dev_eui, measurement_names_weather[0], start, stop)
    #df_bp = influx_test.get_measurement_from_influxdb(bucket, dev_eui, measurement_names_weather[3], start, stop)
    
    #df_gt['value'] = pd.to_numeric(df_gt['value'], errors='coerce')
    #df_incl['value'] = pd.to_numeric(df_incl['value'], errors='coerce')
    #df_airtemp['value'] = pd.to_numeric(df_airtemp['value'], errors='coerce')
    #df_rh['value'] = pd.to_numeric(df_rh['value'], errors='coerce')
    #df_bp['value'] = pd.to_numeric(df_bp['value'], errors='coerce')


    #if df_gt.empty:
    #    logger.debug("No data to plot.")
    #    return

    payload_df = influx_test.get_data_from_measurement(bucket, dev_eui, "device_frmpayload_data_Payload", "device_frmpayload_data_Timestamp", start, stop)
            
    timeseries = influx_test.decode_payload(payload_df,start,stop)

    # Create a Plotly Express figure, with all 5 subplots
    fig = influx_test.all_graphs(timeseries,mast, start, stop)
    
    #fig1 = px.line(
     #   df,
      #  x='time',
       # y='value',
        #color='measurement',
        #title=parameter,
        #labels={'value': 'Temperature (°C)', 'time': 'Time'}
    #)

    #fig1.update_layout(
     #   xaxis_title='Time',
      #  yaxis_title='Temperature (°C)',
       # legend_title='Measurement',
       # template='plotly_white'  # Optional: change the template for aesthetics
    #)

    # Prepare data for the table
    row_data = df_gt.to_dict("records")
    column_defs = [{"field": i} for i in df_gt.columns]

    return fig, row_data, column_defs
    # Plot the measurements over time
    #fig, ax = plt.subplots(figsize=(12, 6))

    # Get unique measurements to plot each with a different color
    #unique_measurements = df['measurement'].unique()
    #for measurement in unique_measurements:
        #subset = df[df['measurement'] == measurement]
        #ax.plot(subset['time'], subset['value'], label=measurement)

    #ax.set_xlabel('Time')
    #ax.set_ylabel('Temperature (°C)')
    #ax.set_title('Measurements Over Time')
    #ax.legend()
    #ax.grid(True)
    #fig.tight_layout()
    # save figure to a file. The filename will be influxdb_<dev_eui>_<timestamp>.png
    #fig.savefig(f"influxdb_{df['dev_eui'].iloc[0]}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.png")
    #plt.show(block=False)
    #return fig



    # except Exception as e:
    #     logger.debug(f"Failed to plot measurements: {e}")
        
    # except Exception as e:
    #     logger.debug(f"Failed to get data from measurement: {e}")
    #     return pd.DataFrame()

    # except Exception as e:
    #     logger.debug(f"Failed to get data from measurement: {e}")
    #     return []



if __name__ == "__main__":
    #app.run()
    app.run_server(debug=DEBUG)
