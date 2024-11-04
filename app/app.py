from dash import Dash, Input, Output, callback, dcc, html
import plotly.express as px
import pandas as pd
from influxdb_client import InfluxDBClient
import dash_ag_grid as dag
import ast
import influx_test



mastopt = ["Mast 6", "Mast 7", "Mast 9", "Mast 12", "Mast 13"]

#Identify the requested mast

# rename to device_eui
codenamesformasts = ['feedbeefcafe0002',     #Mast6 has position 1 in list with options
                    'feedbeefcafe0003',     #Mast7 has position 2 in list with options
                    'feedbeefcafe0001',    #Mast9 has position 3 in list with options
                    'feedbeefcafe0004',     #Mast12 has position 4 in list with options
                    'feedbeefcafe0005']     #Mast13

mast_dict = {'Mast 6': 'feedbeefcafe0002',
             'Mast 7': 'xxxx'}



app = Dash(__name__)

app.layout = html.Div(
    [
        html.H5("Mast"),
        dcc.Dropdown(
            id="mast", options=["Mast 6", "Mast 7", "Mast 9", "Mast 12", "Mast 13"]
        ),
        html.H5("Parameter"),
        dcc.Dropdown(
            id="parameter",
            options=["Ground Temperatures", "Weather data", "Inclination data"]
        ),
        html.Br(),
        dcc.Graph(id="graph"),  # Placeholder for the graph
        dag.AgGrid(  # Placeholder for the table
            id="table",
            columnDefs=[],  # Initialize with no columns
            rowData=[],
            columnSize="sizeToFit",
            defaultColDef={"minWidth": 120, "sortable": True},
            dashGridOptions={"rowSelection": "single"},
        ),
    ]
)


@callback(
    [Output("graph", "figure"), Output("table", "rowData"), Output("table", "columnDefs")],
    [Input("mast", "value"), Input("parameter", "value")],
)

def get_data_from_measurement(mast, parameter):

    print(f"Selected mast: {mast}")
    print(f"Selected parameter: {parameter}")

    start = "2023-01-01T00:00:00Z"
    stop = "now()"
  

    position = mastopt.index(mast),
    
    dev_eui = codenamesformasts[position[0]]

    if parameter == "Ground Temperatures":
            measurement_names = ["device_frmpayload_data_GroundTemp01",
                                "device_frmpayload_data_GroundTemp02",
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
        
    if parameter == "Weather Data":
  
            measurement_names = ["device_frmpayload_data_RelHum",
                                "device_frmpayload_data_AirTemp",
                                "device_frmpayload_data_DewFrostPoint",
                                "device_frmpayload_data_BarometricPressure",
                                "device_frmpayload_data_AbsHum",
                                "device_frmpayload_data_CloudBase",
                                "device_frmpayload_data_Altitude"]
            
    if parameter == "Inclination Data":

            measurement_names = ["device_frmpayload_data_RollAngle",
                            "device_frmpayload_data_PitchAngle",
                            "device_frmpayload_data_CompassHeading"]
    
    timestamp_measurement = "device_frmpayload_data_Timestamp"
    bucket = "data_bucket"

    df = influx_test.get_measurement_from_influxdb(bucket, dev_eui, measurement_names, start, stop)
    
    df['value'] = pd.to_numeric(df['value'], errors='coerce')    

    if df.empty:
        print("No data to plot.")
        return


    # Create a Plotly Express figure
    fig = px.line(
        df,
        x='time',
        y='value',
        color='measurement',
        title=parameter,
        labels={'value': 'Temperature (°C)', 'time': 'Time'}
    )

    fig.update_layout(
        xaxis_title='Time',
        yaxis_title='Temperature (°C)',
        legend_title='Measurement',
        template='plotly_white'  # Optional: change the template for aesthetics
    )

    # Prepare data for the table
    row_data = df.to_dict("records")
    column_defs = [{"field": i} for i in df.columns]

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
    #     print(f"Failed to plot measurements: {e}")
        
    # except Exception as e:
    #     print(f"Failed to get data from measurement: {e}")
    #     return pd.DataFrame()

    # except Exception as e:
    #     print(f"Failed to get data from measurement: {e}")
    #     return []


if __name__ == "__main__":
    app.run()
