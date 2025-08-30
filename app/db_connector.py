import os
import sys
import logging
from pathlib import Path
import django
from django.db import connections
from django.db.models import F
import pandas as pd
import ipdb
import asgiref.sync

# Get the absolute path of the main project folder. It is the folder in which this file lives.
django_project_path = Path(__file__).resolve().parent / 'django_integration'

# Add the Django project directory to sys.path
if django_project_path not in sys.path:
    sys.path.insert(0, str(django_project_path))  # Add the path to the start of sys.path

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'django_integration.core.settings')
django.setup()

# Import Django models
from django_integration.monitoring_db import models


def close_all_stale_db_connections():
    # See https://stackoverflow.com/a/62632743
    for conn in connections.all():
        conn.close_if_unusable_or_obsolete()


# def get_measurements_from_monitoringdb(deveui, sensor_dict, start, stop):
#     """
#     Get measurements from the monitoring database for a specific device and sensor
#     :param dev_eui: The device EUI
#     :param sensor_dict: A dictionary containing the sensor name and the names of the timeseries to fetch from that sensor
#     :param start: The start time of the query
#     :param stop: The stop time of the query
#     :return: A list of measurements
#     """
#     # sensor_dict = [
#     #     {name: xxxx, timeseries: [ts1, ts2, ts3]},
#     #     {name: yyyy, timeseries: [ts4, ts5, ts6]}
#     # ]
    
#     sensor_names = [sensor['name'] for sensor in sensor_dict]

#     rd = models.RecordingDevice.objects.get(deveui=deveui)

#     timeseries_data = {}

#     for name in sensor_names:
#         sensor_obj = rd.sensors.get(name=name)
#         if sensor_obj is None:
#             raise ValueError(f"Sensor {name} not found for device {deveui}")

#         all_ts = sensor_obj.timeseries.filter(name__in=sensor_dict[name]['timeseries'])
#         if not all_ts:
#             raise ValueError(f"No timeseries not found for sensor {name}")
        
#         for ts in all_ts:
#             qs = ts.measurements.filter(time__gte=start, time__lte=stop)
#             pass

#         #     timeseries_data[ts.name] = test
        

#         # for ts_name in sensor['timeseries']:
#         #     ts = sensor_obj.timeseries.get(name=ts_name)
#         #     measurements.extend(ts.measurements.filter(timestamp__gte=start, timestamp__lte=stop))

# define EmptyQuerySet exception
class EmptyQuerySet(Exception):
    pass

class RecordingDeviceNotFound(Exception):
    pass

class SensorNotFound(Exception):
    pass


def qs_to_df(qs):
    df = pd.DataFrame.from_records(qs.values())

    if df.empty:
        raise ValueError("No data found for the given query")

    df.set_index("time", inplace=True)
    df.sort_index(inplace=True)
    df.drop_duplicates(inplace=True)
    return df


def get_sensor_data(deveui, sensor_name, start, stop):
    # using django models.RecordingDevice to find the recording device with the field deveui = dev_eui

    # Call this before any database operation sequence
    close_all_stale_db_connections()

    try:
        rd = models.RecordingDevice.objects.get(deveui=deveui)
    except models.RecordingDevice.DoesNotExist:
        raise RecordingDeviceNotFound("RecordingDevice not found in database")
    
    # The tables are related as follows: RecordingDevice -> Sensor -> Timeseries -> Measurement
    # create a query set of measurements belonging to timeseries that obey the timeseries_name_icontains condition
    # and the sensor_name_icontains condition and belong to the recording device with the deveui = deveui
    qs = models.Measurement.objects.filter(
            timeseries__sensor__recording_device=rd,
            timeseries__sensor__name=sensor_name
    )

    if start is not None:
        qs = qs.filter(time__gte=start)
    if stop is not None:
        qs = qs.filter(time__lte=stop)

    if not qs.exists():
        raise EmptyQuerySet("No data found for the given query")

    qs = qs.annotate(timeseries_name=F('timeseries__name'))

    # convert the query set to a pandas dataframe
    df = qs_to_df(qs)

    # reororganize dataframe so that timeseries names are columns
    df = df.pivot(columns='timeseries_name', values='value')

    # Make some algorithm to return the datatype of each timeseries
    # It could be inserted as a nested column index in the dataframe

    return df



def get_ground_temp(deveui, start, stop):
    # using django models.RecordingDevice to find the recording device with the field deveui = dev_eui
    
    # Call this before any database operation sequence
    close_all_stale_db_connections()

    try:
        rd = models.RecordingDevice.objects.get(deveui=deveui)
    except models.RecordingDevice.DoesNotExist:
        raise RecordingDeviceNotFound("RecordingDevice not found in database")
    
    sensor_name_icontains = 'GroundTemp'
    timeseries_name_icontains = 'GroundTemp'

    # The tables are related as follows: RecordingDevice -> Sensor -> Timeseries -> Measurement
    # create a query set of measurements belonging to timeseries that obey the timeseries_name_icontains condition
    # and the sensor_name_icontains condition and belong to the recording device with the deveui = deveui
    qs = models.Measurement.objects.filter(
            timeseries__sensor__recording_device=rd,
            timeseries__name__icontains=timeseries_name_icontains,
            timeseries__sensor__name__icontains=sensor_name_icontains
    )

    if start is not None:
        qs = qs.filter(time__gte=start)
    if stop is not None:
        qs = qs.filter(time__lte=stop)

    if not qs.exists():
        raise EmptyQuerySet("No data found for the given query")

    qs = qs.annotate(timeseries_name=F('timeseries__name'))

    # convert the query set to a pandas dataframe
    df = qs_to_df(qs)

    # reororganize dataframe so that timeseries names are columns
    df = df.pivot(columns='timeseries_name', values='value')
    datatype = qs.first().datatype

    return df, datatype


def get_ground_temp_sensor_depths(deveui):

    # Call this before any database operation sequence
    close_all_stale_db_connections()

    try:
        rd = models.RecordingDevice.objects.get(deveui=deveui)
    except models.RecordingDevice.DoesNotExist:
        raise RecordingDeviceNotFound("RecordingDevice not found in database")
    
    sensor_name_icontains = 'GroundTemp'

    qs_s = models.Sensor.objects.filter(
        recording_device=rd,
        name__icontains=sensor_name_icontains
    )

    if not qs_s.exists():
        raise EmptyQuerySet("No data found for the given query")    
    
    # get the relative z values of the sensors and sort them in descending order
    sensors_depths = {sensor.name: sensor.relative_z for sensor in qs_s}
    sensors_sorted_by_depth = dict(sorted(sensors_depths.items(), key=lambda item: item[1], reverse=True))

    return sensors_sorted_by_depth


# Async wrappers for Django ORM functions
get_sensor_data_async = asgiref.sync.sync_to_async(get_sensor_data, thread_sensitive=True)
get_ground_temp_async = asgiref.sync.sync_to_async(get_ground_temp, thread_sensitive=True)
get_ground_temp_sensor_depths_async = asgiref.sync.sync_to_async(get_ground_temp_sensor_depths, thread_sensitive=True)
