from influxdb_client import InfluxDBClient
import pandas as pd
from matplotlib import pyplot as plt

# Define your InfluxDB credentials
url = "http://192.38.87.126:8086"  # Replace with your InfluxDB URL
token = "XXXXXXXXXXXX"  # Replace with your InfluxDB token
org = "DTU_SUSTAIN"  # Replace with your InfluxDB organization
DEBUG_MODE = False


def connect_to_influxdb(url, token, org):
    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        health = client.health()
        if health.status == "pass":
            print("Successfully connected to InfluxDB!")
        else:
            print(f"Connection issue: {health.message}")
        return client
    except Exception as e:
        print(f"Failed to connect to InfluxDB: {e}")
        return None

def list_measurements(client, bucket):
    try:
        query_api = client.query_api()
        query = f'import "influxdata/influxdb/schema" schema.measurements(bucket: "{bucket}")'  # Must be on one line
        tables = query_api.query(query)
        measurements = [record.get_value() for table in tables for record in table.records]
        return measurements
    except Exception as e:
        print(f"Failed to list measurements: {e}")
        return []
    
def list_tags(client, bucket, measurement):
    try:
        query_api = client.query_api()
        query = f'import "influxdata/influxdb/schema" schema.tagKeys(bucket: "{bucket}", predicate: (r) => r._measurement == "{measurement}")'
        tables = query_api.query(query)
        tags = [record.get_value() for table in tables for record in table.records]
        return tags
    except Exception as e:
        print(f"Failed to list tags: {e}")
        return []

def get_data_from_measurement(client, bucket, measurements, timestamp_measurement, start, stop):
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
        |> filter(fn: (r) => r["dev_eui"] == "feedbeefcafe0002")
        |> filter(fn: (r) => r["application_name"] == "Transmission Line Kangerluarsuk Ungalleq")
        |> filter(fn: (r) => {measurements_filter})
        //|> filter(fn: (r) => r["_field"] == "value")
        //|> rename(columns: {{_value: "_payload_value"}})  // Rename to avoid conflict during join

        // Query for the timestamp measurement (device_frmpayload_data_Timestamp)
        timestampData = from(bucket: "{bucket}")
        |> range(start: {start}, stop: {stop})
        |> filter(fn: (r) => r["dev_eui"] == "feedbeefcafe0002")
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
        '''
        
        # Additional Flux query steps to clean up the data and prepare for plotting
        # Not currently used, but can be added to the query above if needed

        # // In Flux, when performing a join(), the fields from both tables get renamed with prefixes 
        # // based on the table names (e.g., _value_value, _value_timestamp).

        # //|> map(fn: (r) => ({{
        # //    r with 
        # //    _time: time(v: r["_value_timestamp"]),  // Use the actual timestamp for the _time field
        # //    _value: float(v: r["_value_value"]),  // Ensure _value is correctly assigned as a float for plotting
        # //    _field: r["_field_value"],
        # //    _measurement: r["_measurement_value"]
        # //}}))
        # //// Drop unnecessary columns to clean up the data
        # //|> drop(columns: ["_value_value", "_value_timestamp", "_start_value", "_stop_value", "_measurement_timestamp", "_field_timestamp", "device_name_timestamp", "f_port_timestamp", "_stop_timestamp", "_start_timestamp"])
        # //// Sort the data by _time in ascending order (chronologically)
        # //|> sort(columns: ["_time"], desc: false)
        # |> yield(name: "result")
        

        if DEBUG_MODE:
            print(f"Query:\n{query}")

        tables = query_api.query(query)

        if DEBUG_MODE:
            print(f"Tables:\n{tables}")
            for table in tables:
                print(f"Table:\n{table}")
            for record in tables[0].records:
                print(f"Record:\n{record}")

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
        print(f"Failed to get data from measurement: {e}")
        return pd.DataFrame()

    except Exception as e:
        print(f"Failed to get data from measurement: {e}")
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
        print(f"Failed to get all data from measurement: {e}")
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
            print(f"No '{message_type}' messages found.")
            return None
    except Exception as e:
        print(f"Failed to get last '{message_type}' message: {e}")
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
            print(f"No messages found at timestamp: {timestamp}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Failed to get message by timestamp: {e}")
        return pd.DataFrame()

def plot_measurements(df):
    try:
        if df.empty:
            print("No data to plot.")
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
        print(f"Failed to plot measurements: {e}")

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
        print(f"Query:\n{query}")

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
            print(f"No messages found at timestamp: {timestamp}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Failed to get message by timestamp: {e}")
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
            print(f"No data found for device '{device_name}' in the specified time range.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Failed to retrieve payload timeseries: {e}")
        return pd.DataFrame()
    

if __name__ == "__main__":
    client = connect_to_influxdb(url, token, org)
    if client:
        bucket = "data_bucket"  # Replace with your InfluxDB bucket name

        # Example: Retrieve (names of) all measurements in the bucket
        measurements = list_measurements(client, bucket)
        print("Measurements in the bucket:")
        for measurement in measurements:
            print(measurement)
        
        # Example: Retrieve all tags for a specific measurement
        measurement_name = "device_frmpayload_data_GroundTemp01"
        tags = list_tags(client, bucket, measurement_name)
        print(f"Tags for measurement '{measurement_name}':")
        for tag in tags:
            print(tag)

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
            print(f"Data retreived:")
            # for entry in data:
            #     print(entry)
            print(df)
            
            plot_measurements(df)

        if False:
            # Retrieve all payload timestamps in data_bucket
            measurement_name = "device_frmpayload_data_Timestamp"
            start_time = "2023-01-01T00:00:00Z"
            timestamp_df = get_all_data_from_measurement(client, bucket, measurement_name, start_time)
            print(f"All data from measurement '{measurement_name}':")
            print(timestamp_df)

            # Retrieve all payload MassageType entries in data_bucket
            measurement_name = "device_frmpayload_data_MessageType"
            start_time = "2023-01-01T00:00:00Z"
            mt_df = get_all_data_from_measurement(client, bucket, measurement_name, start_time)
            print(f"All data from measurement '{measurement_name}':")
            print(mt_df)

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
            print("Retrieving data from InfluxDB... Finding specific 'PS' message by index...")
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
            print("Retrieving data from InfluxDB... Finding specific 'PS' message by index...")
            # Retrieve all payload MessageType entries in data_bucket
            device_name = "Tekbox_TBLS1_3"
            start_time = "2023-01-01T00:00:00Z"
            res = get_message_of_type_by_index('PS', -499, start_time)

        if True:

            # Retrieve all payload MassageType entries in data_bucket
            measurement_name = "device_frmpayload_data_Payload"
            start_time = "2023-01-01T00:00:00Z"
            pl_df = get_all_data_from_measurement(client, bucket, measurement_name, start_time)
            with pd.option_context('display.max_rows', None):
                print(pl_df[pl_df['device_name']=='Tekbox_TBLS1_5'][['_time', '_value', 'device_name']])

            # Retrieve a timeseries of the measurement "device_frmpayload_data_Payload"
            start_time = "2023-01-01T00:00:00Z"
            stop_time = "now()"
            device_name = "Tekbox_TBLS1_5"
            payload_df = get_payload_timeseries(client, bucket, start_time, stop_time, device_name)
            print(f"Payload timeseries for device '{device_name}':")
            print(payload_df)

        client.close()
