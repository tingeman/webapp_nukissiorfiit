from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime
from app.config import settings

# Define your InfluxDB credentials
url = settings.influxdb_url
token = settings.influxdb_token
org = settings.influxdb_org
bucket = "test_bucket"  # Replace with your InfluxDB bucket name

# Payload data
payload = {
    "MessageType": "Ctest3",
    "rssi": "-83",
    "SensorNr": "3",
    "fPort": 8,
    "Payload": "C24:11:02:21:00:00000001010400013933T -83",
    "BoardStatus": "T",
    "SupplyID": "1",
    "FWversion": "04000139",
    "DeviceID": "99999999",
    "Timestamp": "2024-11-02T21:00:00Z"
}

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

def write_payload(client, bucket, payload):
    try:
        write_api = client.write_api()
        point = Point("measurement_name") \
            .tag("MessageType", payload["MessageType"]) \
            .tag("SensorNr", payload["SensorNr"]) \
            .tag("BoardStatus", payload["BoardStatus"]) \
            .tag("SupplyID", payload["SupplyID"]) \
            .tag("FWversion", payload["FWversion"]) \
            .tag("DeviceID", payload["DeviceID"]) \
            .field("rssi", int(payload["rssi"])) \
            .field("fPort", payload["fPort"]) \
            .field("Payload", payload["Payload"]) \
            .time(datetime.strptime(payload["Timestamp"], "%Y-%m-%dT%H:%M:%SZ"), WritePrecision.S)
        
        write_api.write(bucket=bucket, org=org, record=point)
        print("Payload written successfully!")
    except Exception as e:
        print(f"Failed to write payload: {e}")

if __name__ == "__main__":
    client = connect_to_influxdb(url, token, org)
    if client:
        write_payload(client, bucket, payload)