from influxdb_client import InfluxDBClient
import pandas as pd
from matplotlib import pyplot as plt
import plotly.express as px
import dash_bootstrap_components as dbc
from dash import Dash, Input, Output, callback, dcc, html
from collections import defaultdict
import re
import plotly.graph_objects as go
from datetime import datetime

# Define your InfluxDB credentials
from config import settings

url = settings.influxdb_url
token = settings.influxdb_token
org = settings.influxdb_org
DEBUG_MODE = settings.DEBUG

# set up logging to console
import logging
if settings.DEBUG:
    logging.basicConfig(level=logging.DEBUG, force=True)
else:
    logging.basicConfig(level=logging.INFO, force=True)
logger = logging.getLogger(__name__)

# url = ""
# token = ""
# org = ""
# DEBUG_MODE = True

#Legend entries for ground temperatures:

depths_sensors = {
    "GroundTemp01": "10 cm",
    "GroundTemp02": "25 cm",
    "GroundTemp03": "50 cm",
    "GroundTemp04": "75 cm",
    "GroundTemp05": "100 cm",
    "GroundTemp06": "125 cm",
    "GroundTemp07": "150 cm",
    "GroundTemp08": "175 cm",
    "GroundTemp09": "200 cm",
    "GroundTemp10": "225 cm",
    "GroundTemp11": "250 cm",
    "GroundTemp12": "300 cm",
}


def all_graphs(timeseries, mast, start, stop):

    #for category in timeseries:
    #    timeseries[category] = {datetime.strptime(timestamp, r"%Y-%m-%d %H:%M:%S"): values for timestamp, values in timeseries[category].items()}

    #filtered_data = {}
    #for category, timeserie in timeseries.items():
    #    filtered_data[category] = {timestamp: values for timestamp, values in timeserie.items() if start <= timestamp <= stop}
 
    #timeseries = filtered_data

    ground_temperatures = timeseries["Ground Temperatures"]
    weather_data = timeseries["Weather Data"]
    inclination_data = timeseries["Inclination Data"]
    battery_data = timeseries["Battery Levels"]

    # 1. Ground Temperatures Plot
    fig_gt = go.Figure()
    #logger.debug(ground_temperatures.columns)
    for column in ground_temperatures.columns:
            fig_gt.add_trace(go.Scatter(
                x=ground_temperatures.index,  # X-axis (Timestamp)
                y=ground_temperatures[column],  # Y-axis (Temperature values)
                mode='lines+markers',  # Lines and markers
                name=depths_sensors[column],  # Label for each line (Depth sensor)
                marker=dict(size=6)  # Marker customization
            ))

    fig_gt.update_layout(
            title=f"Ground Temperature - {mast}",
            xaxis_title='Time',
            yaxis_title='Temperature (°C)',
            legend_title='Sensor Depth',
            template='plotly_white'
        )

    fig_airtemp = go.Figure()

    fig_airtemp.add_trace(go.Scatter(
                x=weather_data.index,  # X-axis (Timestamp)
                y=weather_data["AirTemp"],  # Y-axis (Temperature values)
                mode='lines+markers',  # Lines and markers
                name=depths_sensors[column],  # Label for each line (Depth sensor)
                marker=dict(size=6)  # Marker customization
            ))

    fig_airtemp.update_layout(
        xaxis_title='Time',
        yaxis_title='Air Temperature (°C)',
        showlegend=False,
        template='plotly_white'
    )

    fig_rh = go.Figure()

    fig_rh.add_trace(go.Scatter(
                x=weather_data.index,  # X-axis (Timestamp)
                y=weather_data["RelHum"],  # Y-axis (Temperature values)
                mode='lines+markers',  # Lines and markers
                name=depths_sensors[column],  # Label for each line (Depth sensor)
                marker=dict(size=6)  # Marker customization
            ))
    
    fig_rh.update_layout(
        xaxis_title='Time',
        yaxis_title='Relative Humidity (%)',
        showlegend=False,
        template='plotly_white'
    )

    fig_bp = go.Figure()

    fig_bp.add_trace(go.Scatter(
                x=weather_data.index,  # X-axis (Timestamp)
                y=weather_data["BarometricPressure"],  # Y-axis (Temperature values)
                mode='lines+markers',  # Lines and markers
                name=depths_sensors[column],  # Label for each line (Depth sensor)
                marker=dict(size=6)  # Marker customization
            ))
    
    fig_bp.update_layout(
        xaxis_title='Time',
        yaxis_title='Barometric Pressure (kPa)',
        showlegend=False,
        template='plotly_white'
    )

    fig_incl = go.Figure()
    
    for column in inclination_data.columns:
            fig_incl.add_trace(go.Scatter(
                x=inclination_data.index,  # X-axis (Timestamp)
                y=inclination_data[column],  # Y-axis (Temperature values)
                mode='lines+markers',  # Lines and markers
                name=column,  # Label for each line (Depth sensor)
                marker=dict(size=6)  # Marker customization
            ))

    fig_incl.update_layout(
        xaxis_title='Time',
        yaxis_title='Inclinations',
        legend_title='Measurement',
        template='plotly_white'
    )

    graph1 = dcc.Graph(figure=fig_gt, className="border")
    graph2 = dcc.Graph(figure=fig_airtemp, className="border")
    graph3 = dcc.Graph(figure=fig_rh, className="border")
    graph4 = dcc.Graph(figure=fig_bp, className="border")
    graph5 = dcc.Graph(figure=fig_incl, className="border")

    return [
        dbc.Row([dbc.Col(graph1, lg=6), dbc.Col(graph2, lg=6)]),
        dbc.Row([dbc.Col(graph3, lg=6), dbc.Col(graph4, lg=6)]),
        dbc.Row([dbc.Col(graph5, lg=6)], className="mt-4"),
    ]



def connect_to_influxdb(url, token, org):
    try:
        client = InfluxDBClient(url=url, token=token, org=org, timeout=60_000)
        health = client.health()
        if health.status == "pass":
            logger.debug("Successfully connected to InfluxDB!")
        else:
            logger.debug(f"Connection issue: {health.message}")
        return client
    except Exception as e:
        logger.debug(f"Failed to connect to InfluxDB: {e}")
        return None

def list_measurements(client, bucket):
    try:
        query_api = client.query_api()
        query = f'import "influxdata/influxdb/schema" schema.measurements(bucket: "{bucket}")'  # Must be on one line
        tables = query_api.query(query)
        measurements = [record.get_value() for table in tables for record in table.records]
        return measurements
    except Exception as e:
        logger.debug(f"Failed to list measurements: {e}")
        return []
    
def list_tags(client, bucket, measurement):
    try:
        query_api = client.query_api()
        query = f'import "influxdata/influxdb/schema" schema.tagKeys(bucket: "{bucket}", predicate: (r) => r._measurement == "{measurement}")'
        tables = query_api.query(query)
        tags = [record.get_value() for table in tables for record in table.records]
        return tags
    except Exception as e:
        logger.debug(f"Failed to list tags: {e}")
        return []


#def get_measurement_from_influxdb(bucket, dev_eui, measurements, timestamp_measurement, start, stop='now()'):    
def get_measurement_from_influxdb(bucket, dev_eui, measurements, start, stop='now()'):
    
    logger.debug(f"Getting data from InfluxDB for device {dev_eui}...")
    logger.debug(f"Start: {start}\n Stop: {stop}")

    client = connect_to_influxdb(url, token, org)
    if client is None:
        logger.debug("Failed to connect to InfluxDB.")
        logger.debug(f"url: {url}\n token: {token}\n org: {org}")

    query_api = client.query_api()
    
    if isinstance(measurements, str):
        measurements_filter = f'r["_measurement"] == "{measurements}"'
    else:
        measurements_filter = " or ".join([f'r["_measurement"] == "{measurement}"' for measurement in measurements])
    
    query = f'''
    // Query for the value measurements
    payloadData = from(bucket: "{bucket}")
    |> range(start: {start}, stop: {stop})
    |> filter(fn: (r) => r["dev_eui"] == "{dev_eui}")
    |> filter(fn: (r) => r["application_name"] == "Transmission Line Kangerluarsuk Ungalleq")
    |> filter(fn: (r) => {measurements_filter})
    //|> filter(fn: (r) => r["_field"] == "value")
    //|> rename(columns: {{_value: "_payload_value"}})  // Rename to avoid conflict during join

    // Query for the timestamp measurement (device_frmpayload_data_Timestamp)
    timestampData = from(bucket: "{bucket}")
    |> range(start: {start}, stop: {stop})
    |> filter(fn: (r) => r["dev_eui"] == "{dev_eui}")
    |> filter(fn: (r) => r["application_name"] == "Transmission Line Kangerluarsuk Ungalleq")
    |> filter(fn: (r) => r["_measurement"] == "device_frmpayload_data_Timestamp")
    //|> filter(fn: (r) => r["_field"] == "value")
    // Keep only certain fields from the timestampData (for example, _value and _time)
    |> keep(columns: ["dev_eui", "_time", "_value"])
    |> rename(columns: {{_value: "_payload_timestamp"}})  // Rename timestamp value

    // Join the value and timestamp data
    join(
    tables: {{payload: payloadData, timestamp: timestampData}},
    on: ["dev_eui", "_time"]
    )
    |> yield(name: "result")
    '''

    if DEBUG_MODE:
        logger.debug(f"Query:\n{query}")

    tables = query_api.query(query)

    if DEBUG_MODE:
        logger.debug(f"Tables:\n{tables}")
        for table in tables:
            logger.debug(f"Table:\n{table}")
        for record in tables[0].records:
            logger.debug(f"Record:\n{record}")

    # data = [
    #     {
    #         "time": record.get_time(),
    #         "field": record.get_field(),
    #         "value": record.get_value()
    #     }
    #     for table in tables for record in table.records
    # ]
    # return data

            # Convert to Pandas DataFrame, retaining all columns
    data = []
    for table in tables:
        for record in table.records:
            record_dict = {
                "time": record.get_time(),
                "measurement": record.get_measurement(),
                "field": record.get_field(),
                "value": record.get_value(),
            }
            record_dict.update(record.values)  # Add all other columns/attributes
            data.append(record_dict)
    
    df = pd.DataFrame(data)
    return df


def get_data_from_measurement(bucket, dev_eui, measurements, timestamp_measurement, start, stop):

    client = connect_to_influxdb(url, token, org)
    
    
    try:
        query_api = client.query_api()
        if isinstance(measurements, str):
            measurements_filter = f'r["_measurement"] == "{measurements}"'
        else:
            measurements_filter = " or ".join([f'r["_measurement"] == "{measurement}"' for measurement in measurements])

        query = f'''
        // Query for the value measurements
        payloadData = from(bucket: "{bucket}")
        |> range(start: {start}, stop: {stop})
        |> filter(fn: (r) => r["dev_eui"] == "{dev_eui}")
        |> filter(fn: (r) => r["application_name"] == "Transmission Line Kangerluarsuk Ungalleq")
        |> filter(fn: (r) => {measurements_filter})
        //|> filter(fn: (r) => r["_field"] == "value")
        //|> rename(columns: {{_value: "_payload_value"}})  // Rename to avoid conflict during join

        // Query for the timestamp measurement (device_frmpayload_data_Timestamp)
        timestampData = from(bucket: "{bucket}")
        |> range(start: {start}, stop: {stop})
        |> filter(fn: (r) => r["dev_eui"] == "{dev_eui}")
        |> filter(fn: (r) => r["application_name"] == "Transmission Line Kangerluarsuk Ungalleq")
        |> filter(fn: (r) => r["_measurement"] == "device_frmpayload_data_Timestamp")
        //|> filter(fn: (r) => r["_field"] == "value")
        // Keep only certain fields from the timestampData (for example, _value and _time)
        |> keep(columns: ["dev_eui", "_time", "_value"])
        |> rename(columns: {{_value: "_payload_timestamp"}})  // Rename timestamp value

        // Join the value and timestamp data
        join(
        tables: {{payload: payloadData, timestamp: timestampData}},
        on: ["dev_eui", "_time"]
        )

        // In Flux, when performing a join(), the fields from both tables get renamed with prefixes 
        // based on the table names (e.g., _value_value, _value_timestamp).

        //|> map(fn: (r) => ({{
        //    r with 
        //    _time: time(v: r["_value_timestamp"]),  // Use the actual timestamp for the _time field
        //    _value: float(v: r["_value_value"]),  // Ensure _value is correctly assigned as a float for plotting
        //    _field: r["_field_value"],
        //    _measurement: r["_measurement_value"]
        //}}))
        //// Drop unnecessary columns to clean up the data
        //|> drop(columns: ["_value_value", "_value_timestamp", "_start_value", "_stop_value", "_measurement_timestamp", "_field_timestamp", "device_name_timestamp", "f_port_timestamp", "_stop_timestamp", "_start_timestamp"])
        //// Sort the data by _time in ascending order (chronologically)
        //|> sort(columns: ["_time"], desc: false)
        |> yield(name: "result")
        '''

        if DEBUG_MODE:
            logger.debug(f"Query:\n{query}")

        tables = query_api.query(query)

        if DEBUG_MODE:
            logger.debug(f"Tables:\n{tables}")
            for table in tables:
                logger.debug(f"Table:\n{table}")
            for record in tables[0].records:
                logger.debug(f"Record:\n{record}")

        # data = [
        #     {
        #         "time": record.get_time(),
        #         "field": record.get_field(),
        #         "value": record.get_value()
        #     }
        #     for table in tables for record in table.records
        # ]
        # return data

                # Convert to Pandas DataFrame, retaining all columns
        data = []
        for table in tables:
            for record in table.records:
                record_dict = {
                    "time": record.get_time(),
                    "measurement": record.get_measurement(),
                    "field": record.get_field(),
                    "value": record.get_value(),
                }
                record_dict.update(record.values)  # Add all other columns/attributes
                data.append(record_dict)
        df = pd.DataFrame(data)
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        return df
    except Exception as e:
        logger.debug(f"Failed to get data from measurement: {e}")
        return pd.DataFrame()

    except Exception as e:
        logger.debug(f"Failed to get data from measurement: {e}")
        return []

def get_all_data_from_measurement(client, bucket, measurement, start):
    query = f'''
    from(bucket: "{bucket}")
    |> range(start: {start}, stop: now())
    |> filter(fn: (r) => r["_measurement"] == "{measurement}")
    '''
    try:
        query_api = client.query_api()
        tables = query_api.query(query)
    except Exception as e:
        logger.debug(f"Failed to get all data from measurement: {e}")
        return pd.DataFrame()

    # Convert to Pandas DataFrame, retaining all columns
    data = []
    for table in tables:
        for record in table.records:
            # record_dict = {
            #     "time": record.get_time(),
            #     "measurement": record.get_measurement(),
            #     "field": record.get_field(),
            #     "value": record.get_value(),
            # }
            # record_dict.update(record.values)  # Add all other columns/attributes
            # data.append(record_dict)
            data.append(record.values)
    df = pd.DataFrame(data)
    #df['value'] = pd.to_numeric(df['_value'], errors='coerce')
    return df

def get_last_message_of_type(df, message_type):
    try:
        message_df = df[df['_value'] == message_type]
        if not message_df.empty:
            last_message_row = message_df.loc[message_df['_time'].idxmax()]
            return last_message_row
        else:
            logger.debug(f"No '{message_type}' messages found.")
            return None
    except Exception as e:
        logger.debug(f"Failed to get last '{message_type}' message: {e}")
        return None

def get_message_of_type_by_index(message_type, idx, start, stop='now()'):

    if idx < 0:
        # an idx of -1 corresponds to the last message, -2 to the second to last, etc.
        # thus an offset of 0 corresponds to the last message, 1 to the second to last, etc.
        offset = abs(idx) - 1   # Convert to positive index and subtract 1 for offset
        query = f'''
        // Step 1: Get the entry's _time where device_frmpayload_data_MessageType == 'PS' and device_name matches
        timestamp = from(bucket: "data_bucket")
        |> range(start: {start}, stop: {stop})
        |> filter(fn: (r) => r._measurement == "device_frmpayload_data_MessageType" and r._field == "value" and r._value == "{message_type}" and r.device_name == "Tekbox_TBLS1_5")
        |> sort(columns: ["_time"], desc: true) // Sort by _time in descending order
        |> limit(n: 1, offset: {offset}) // Select the entry in descending order 
        |> keep(columns: ["_time"])
        '''
    else:
        offset = idx-1
        query = f'''
        // Step 1: Get the entry's _time where device_frmpayload_data_MessageType == 'PS' and device_name matches
        timestamp = from(bucket: "data_bucket")
        |> range(start: {start}, stop: {stop})
        |> filter(fn: (r) => r._measurement == "device_frmpayload_data_MessageType" and r._field == "value" and r._value == "{message_type}" and r.device_name == "Tekbox_TBLS1_5")
        |> sort(columns: ["_time"], desc: false) // Sort by _time in ascending order
        |> limit(n: 1, offset: {offset}) // Select the entry in ascending order
        |> keep(columns: ["_time"])
        '''
        
    query = query + f'''
        // Step 2: Extract the _time from the selected entry
        entry_time = timestamp |> findColumn(fn: (key) => true, column: "_time")

        // Step 3: Find all measurements with the same _time
        from(bucket: "data_bucket")
        |> range(start: {start}, stop: {stop})
        |> filter(fn: (r) => r._time == entry_time[0]) // Using the timestamp from the selected entry
        '''
    try:
        query_api = client.query_api()
        tables = query_api.query(query)
        if tables:
            data = []
            for table in tables:
                for record in table.records:
                    data.append(record.values)
            df = pd.DataFrame(data)
            return df
        else:
            logger.debug(f"No messages found at timestamp: {timestamp}")
            return pd.DataFrame()
    except Exception as e:
        logger.debug(f"Failed to get message by timestamp: {e}")
        return pd.DataFrame()

def plot_measurements(df):
    try:
        if df.empty:
            logger.debug("No data to plot.")
            return

        # Plot the measurements over time
        fig, ax = plt.subplots(figsize=(12, 6))

        # Get unique measurements to plot each with a different color
        unique_measurements = df['measurement'].unique()
        for measurement in unique_measurements:
            subset = df[df['measurement'] == measurement]
            ax.plot(subset['time'], subset['value'], label=measurement)

        ax.set_xlabel('Time')
        ax.set_ylabel('Temperature (°C)')
        ax.set_title('Measurements Over Time')
        ax.legend()
        ax.grid(True)
        fig.tight_layout()
        # save figure to a file. The filename will be influxdb_<dev_eui>_<timestamp>.png
        fig.savefig(f"influxdb_{df['dev_eui'].iloc[0]}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.png")
        plt.show(block=False)
    except Exception as e:
        logger.debug(f"Failed to plot measurements: {e}")

def get_entries_by_timestamp(client, bucket, timestamp: str, dev_eui: str = None, device_name: str = None):
    if dev_eui:
        device_filter = f'r["dev_eui"] == "{dev_eui}"'
    elif device_name:
        device_filter = f'r["device_name"] == "{device_name}"'
    else:
        raise ValueError("Either dev_eui or device_name must be provided.")

    query = f'''
    from(bucket: "{bucket}")
    |> range(start: {timestamp}, stop: now())
    |> filter(fn: (r) => r._time == {timestamp})
    |> filter(fn: (r) => {device_filter})
    '''
    if DEBUG_MODE:
        logger.debug(f"Query:\n{query}")

    try:
        query_api = client.query_api()
        tables = query_api.query(query)
        if tables:
            data = []
            for table in tables:
                for record in table.records:
                    data.append(record.values)
            df = pd.DataFrame(data)
            return df
        else:
            logger.debug(f"No messages found at timestamp: {timestamp}")
            return pd.DataFrame()
    except Exception as e:
        logger.debug(f"Failed to get message by timestamp: {e}")
        return pd.DataFrame()

def get_payload_timeseries(client, bucket, start_time, stop_time, device_name):
    query = f'''
    from(bucket: "{bucket}")
    |> range(start: {start_time}, stop: {stop_time})
    |> filter(fn: (r) => r["_measurement"] == "device_frmpayload_data_Payload")
    |> filter(fn: (r) => r["device_name"] == "{device_name}")
    |> keep(columns: ["_time", "_value", "device_name"])
    |> sort(columns: ["_time"], desc: false)
    '''
    try:
        query_api = client.query_api()
        tables = query_api.query(query)
        if tables:
            data = []
            for table in tables:
                for record in table.records:
                    data.append(record.values)
            df = pd.DataFrame(data)
            return df
        else:
            logger.debug(f"No data found for device '{device_name}' in the specified time range.")
            return pd.DataFrame()
    except Exception as e:
        logger.debug(f"Failed to retrieve payload timeseries: {e}")
        return pd.DataFrame()

# Code to decode payload instead of the measurements as saved in InfluxDB


# Define the regular expressions
re_message_type = re.compile(r"^(?P<msgtype>PS|PB|PA|PP|C).*")
re_timestamp = re.compile(r"^(?:\D\D{0,1})(?P<year>\d\d)(?:\:)(?P<month>\d\d)(?:\:)(?P<day>\d\d)(?:\:)(?P<hour>\d\d)(?:\:)(?P<min>\d\d)(?:\:)(?P<sec>\d\d)(?:.*)")
re_sensor_id = re.compile(r"^(?:PS|PA|PP)(?:.{17})(?P<sensor>\d)(?:.*)")
re_sub_sensor_id = re.compile(r"^(?:PS|PA|PP)(?:.{18})(?P<subsensor>\d)(?:.*)")
re_C_rssi = re.compile(r"^(?:C)(?:.{37})(?P<rssi>.*)")
re_PB_battery_level = re.compile(r"^(?:PB)(?:.{18})(?P<battery>.*)")
re_PS_data_count = re.compile(r"^(?:PS)(?:.{19})(?P<count>\d\d)(?:.*)")
re_PS_data_values = re.compile(r"^(?:PS)(?:.{22})(?P<values>.*)")
re_C_device_id = re.compile(r"^(?:C)(?:.{17})(?P<devid>.{8})")
re_C_fw_version = re.compile(r"^(?:C)(?:.{25})(?P<fwv>.{8})")
re_C_power_suppl = re.compile(r"^(?:C)(?:.{33})(?P<pwrsuppl>.{1})")
re_C_sensor_nr = re.compile(r"^(?:C)(?:.{34})(?P<sensnr>.{1})")
re_C_board_st = re.compile(r"^(?:C)(?:.{35})(?P<boardst>.{1})")

def extract_data_values_ps(data_sum, count):
    values_match = re_PS_data_values.search(data_sum)
    if values_match:
        values_str = values_match.group("values")
        #logger.debug(f"Extracted values: {values_str}")
        return list(map(float, values_str.split()[:count]))
    return []

def extract_data_values_pb(data_sum):
    battery_match = re_PB_battery_level.search(data_sum)
    if battery_match:
        battery_level = battery_match.group("battery")
        #logger.debug(f"Extracted battery level: {battery_level}")
        return float(battery_level)
    return None

def decode_payload(payloads_df, start, stop):
    # Create dictionaries to store data for each table type
    
    ground_temperatures = defaultdict(list)
    weather_data = defaultdict(list)
    inclination_data = defaultdict(list)
    battery_levels = defaultdict(list)

    payloads = payloads_df._value
    
    for data_sum in payloads:
        #logger.debug(f"Processing payload: {data_sum}")
        
        # Extract the message type
        message_type_match = re_message_type.match(data_sum)
        if message_type_match:
            message_type = message_type_match.group("msgtype")
            #logger.debug(f"Message type: {message_type}")
        else:
            logger.debug("No message type found.")
            continue
        
        # Extract and format timestamp
        timestamp_match = re_timestamp.search(data_sum)
        if timestamp_match:
            timestamp = f"20{timestamp_match.group('year')}-{timestamp_match.group('month')}-{timestamp_match.group('day')} {timestamp_match.group('hour')}:{timestamp_match.group('min')}:{timestamp_match.group('sec')}"
            #logger.debug(f"Timestamp: {timestamp}")
        else:
            logger.debug("No timestamp found.")
            continue

        # Extract Sensor ID and SubSensor ID
        sensor_id_match = re_sensor_id.search(data_sum)
        subsensor_id_match = re_sub_sensor_id.search(data_sum)
        
        if sensor_id_match and subsensor_id_match:
            sensor_id = int(sensor_id_match.group("sensor"))
            subsensor_id = int(subsensor_id_match.group("subsensor"))
            #logger.debug(f"Sensor ID: {sensor_id}, SubSensor ID: {subsensor_id}")
        #else:
            #logger.debug("No sensor or subsensor ID found.")
            

        # Decode based on sensor type and message type
        if message_type == "PS":
            data_count_match = re_PS_data_count.search(data_sum)
            if data_count_match:
                data_count = int(data_count_match.group("count"))
                values = extract_data_values_ps(data_sum, data_count)
                
                # Assign data to the correct table based on Sensor ID and SubSensor ID
                if sensor_id == 0 and subsensor_id == 0:
                    inclination_data["RollAngle"].append((timestamp, values[0]))
                    inclination_data["PitchAngle"].append((timestamp, values[1]))
                    inclination_data["CompassHeading"].append((timestamp, values[2]))
                elif sensor_id == 0 and subsensor_id == 1:
                    inclination_data["InternalTemp"].append((timestamp, values[0]))
                elif sensor_id == 1 and subsensor_id == 0:
                    for i in range(9):
                        ground_temperatures[f"GroundTemp{i+1:02}"].append((timestamp, values[i]))
                elif sensor_id == 1 and subsensor_id == 1:
                    for i in range(3):
                        ground_temperatures[f"GroundTemp{i+10:02}"].append((timestamp, values[i]))
                elif sensor_id == 2 and subsensor_id == 0:
                    weather_data["RelHum"].append((timestamp, values[0]))
                    weather_data["AirTemp"].append((timestamp, values[1]))
                    weather_data["DewFrostPoint"].append((timestamp, values[2]))
                    weather_data["BarometricPressure"].append((timestamp, values[3]))
                elif sensor_id == 2 and subsensor_id == 1:
                    weather_data["AbsHum"].append((timestamp, values[0]))
                    weather_data["CloudBase"].append((timestamp, values[1]))
                    weather_data["Altitude"].append((timestamp, values[2]))
        
        #logger.debug(message_type == "PB")

        if message_type == "PB":
            battery_level = extract_data_values_pb(data_sum)
            #logger.debug(battery_level)
            if battery_level is not None:
                battery_levels["Battery"].append((timestamp, battery_level))
        
    # Helper function to convert data dictionary to DataFrame
    def process_data(data_dict):
        processed_data = defaultdict(list)
        
        # Gather all unique timestamps
        timestamps = sorted(set(time for entries in data_dict.values() for time, _ in entries))
        
        if not timestamps:
            return pd.DataFrame()  # Return empty DataFrame if no timestamps

        # Populate processed_data with timestamps and values
        for timestamp in timestamps:
            processed_data["Timestamp"].append(timestamp)
            for param, entries in data_dict.items():
                # Get value for this timestamp if available, else None
                value = next((v for t, v in entries if t == timestamp), None)
                processed_data[param].append(value)
        
        # Convert to DataFrame and set 'Timestamp' as the index
        df = pd.DataFrame(processed_data)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
        #Filter based on start and stop. Necessary as payloads sent between start and stop often contain payloads from earlier on
        df_filtered = df[(df["Timestamp"] >= start) & (df["Timestamp"] <= stop)]
        #logger.debug(df_filtered)
        return df_filtered.set_index("Timestamp")
    #logger.debug(ground_temperatures)
    # Process each data dictionary and convert to DataFrame with Timestamp index
    data_dict = {
        "Ground Temperatures": process_data(ground_temperatures),
        "Weather Data": process_data(weather_data),
        "Inclination Data": process_data(inclination_data),
        "Battery Levels": process_data(battery_levels)
    }
    #gt_test = data_dict["Ground Temperatures"]
    #logger.debug(gt_test.index.dtype)
    return data_dict

if __name__ == "__main__":
    client = connect_to_influxdb(url, token, org, timeout=60_000)
    if client:
        bucket = "data_bucket"  # Replace with your InfluxDB bucket name

        # Example: Retrieve (names of) all measurements in the bucket
        measurements = list_measurements(client, bucket)
        logger.debug("Measurements in the bucket:")
        for measurement in measurements:
            logger.debug(measurement)
        
        # Example: Retrieve all tags for a specific measurement
        measurement_name = "device_frmpayload_data_GroundTemp01"
        tags = list_tags(client, bucket, measurement_name)
        logger.debug(f"Tags for measurement '{measurement_name}':")
        for tag in tags:
            logger.debug(tag)

        if False:
            # Example: Retrieve all data from a specific measurement within a given time range
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
                                "device_frmpayload_data_GroundTemp12",]
            timestamp_measurement = "device_frmpayload_data_Timestamp"
            start_time = "2024-10-01T00:00:00Z"
            stop_time = "now()"
            df = get_data_from_measurement(client, bucket, measurement_names, timestamp_measurement, start_time, stop_time)
            logger.debug(f"Data retreived:")
            # for entry in data:
            #     logger.debug(entry)
            logger.debug(df)
            
            plot_measurements(df)

        if False:
            # Retrieve all payload timestamps in data_bucket
            measurement_name = "device_frmpayload_data_Timestamp"
            start_time = "2023-01-01T00:00:00Z"
            timestamp_df = get_all_data_from_measurement(client, bucket, measurement_name, start_time)
            logger.debug(f"All data from measurement '{measurement_name}':")
            logger.debug(timestamp_df)

            # Retrieve all payload MassageType entries in data_bucket
            measurement_name = "device_frmpayload_data_MessageType"
            start_time = "2023-01-01T00:00:00Z"
            mt_df = get_all_data_from_measurement(client, bucket, measurement_name, start_time)
            logger.debug(f"All data from measurement '{measurement_name}':")
            logger.debug(mt_df)

        if False:
            # Retrieve all payload timestamps in data_bucket
            measurement_name = "device_frmpayload_data_Timestamp"
            start_time = "2023-01-01T00:00:00Z"
            timestamp_df = get_all_data_from_measurement(client, bucket, measurement_name, start_time)
            timestamp_df['_value'] = pd.to_datetime(timestamp_df['_value'])
            timestamp_df['plot_val'] = 1  # set an arbitrary value to plot
            timestamp_df.plot(x='_value', y='plot_val', style='.')
            plt.xlabel('Time')
            plt.title('Payload timestamps stored in InfluxDB')

        if False:
            # Plot all payload MessageType entries in data_bucket as function of payload timestamp
            logger.debug("Retrieving data from InfluxDB... Finding specific 'PS' message by index...")
            # Retrieve all payload MessageType entries in data_bucket
            measurement_name = "device_frmpayload_data_MessageType"
            start_time = "2023-01-01T00:00:00Z"
            mt_df = get_all_data_from_measurement(client, bucket, measurement_name, start_time)

            # limit data to a specific device_name
            device_name = "Tekbox_TBLS1_4"

            # If device_name is defined and not None/empty, filter the data to only include entries with that device_name
            if device_name:
                mt_df = mt_df[mt_df['device_name'] == device_name]
            
            # add a column plot_val to the dataframe, containing a different value for each MessageType
            mt_df['plot_val'] = mt_df['_value'].astype('category').cat.codes
            mt_df.plot(x='_time', y='plot_val', style='.')
            # Ensure y-axis ticks are labeled with the correct category labels
            plt.yticks(ticks=range(len(mt_df['_value'].astype('category').cat.categories)),
                       labels=mt_df['_value'].astype('category').cat.categories)
            plt.xlabel('Time')
            plt.ylabel('MessageType')

            if device_name:
                plt.title(f'Payload MessageTypes stored in InfluxDB for device_name: {device_name}')
            else:
                plt.title('Payload MessageTypes stored in InfluxDB (all devices)')
            plt.show(block=False)

        if False:
            logger.debug("Retrieving data from InfluxDB... Finding specific 'PS' message by index...")
            # Retrieve all payload MessageType entries in data_bucket
            device_name = "Tekbox_TBLS1_3"
            start_time = "2023-01-01T00:00:00Z"
            res = get_message_of_type_by_index('PS', -499, start_time)

        if False:

            # Retrieve all payload MassageType entries in data_bucket
            measurement_name = "device_frmpayload_data_Payload"
            start_time = "2023-01-01T00:00:00Z"
            pl_df = get_all_data_from_measurement(client, bucket, measurement_name, start_time)
            with pd.option_context('display.max_rows', None):
                logger.debug(pl_df[pl_df['device_name']=='Tekbox_TBLS1_5'][['_time', '_value', 'device_name']])

            # Retrieve a timeseries of the measurement "device_frmpayload_data_Payload"
            start_time = "2023-01-01T00:00:00Z"
            stop_time = "now()"
            device_name = "Tekbox_TBLS1_5"
            payload_df = get_payload_timeseries(client, bucket, start_time, stop_time, device_name)
            logger.debug(f"Payload timeseries for device '{device_name}':")
            logger.debug(payload_df)

        client.close()
