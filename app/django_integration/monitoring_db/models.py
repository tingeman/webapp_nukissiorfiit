# from django.db import models
from django.contrib.gis.db import models
from django.utils.timezone import now
from django.conf import settings
from timescale.db.models.models import TimescaleModel


class BaseModel(models.Model):
    created_at = models.DateTimeField(default=now)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name="%(class)s_created_by")
    modified_at = models.DateTimeField(auto_now=True)
    modified_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name="%(class)s_modified_by")

    class Meta:
        abstract = True


class DataType(BaseModel):
    name = models.CharField(max_length=255)
    symbol = models.CharField(max_length=50, null=True, blank=True)  # Symbol of the data type
    unit = models.CharField(max_length=50)
    description = models.TextField(null=True, blank=True)  # Description of the data type
    valid_range_min = models.FloatField(null=True, blank=True)  # Minimum valid value for the data type
    valid_range_max = models.FloatField(null=True, blank=True)  # Maximum valid value for the data type

    def __str__(self):
        return f"{self.name} ({self.unit})"


class Location(BaseModel):
    class PositionQuality(models.IntegerChoices):
        UNKNOWN = 0, 'Unknown'
        APPROX = 1, 'Approximate'
        DIGITIZED = 2, 'Digitized'
        GNSSCODE = 3, 'GNSS Code'
        GNSSFIXED = 4, 'GNSS Fixed'
        GNSSDIFF = 5, 'GNSS Differential'
        
    name = models.CharField(max_length=255)
    geometry = models.GeometryField()  # PostGIS geometry field for flexible geometry types
    elevation = models.FloatField(null=True, blank=True)  # Elevation in meters
    position_quality = models.IntegerField(choices=PositionQuality.choices, default=PositionQuality.UNKNOWN, null=True, blank=True)  # Enum field for position quality
    slope = models.FloatField(null=True, blank=True)  # Slope in degrees
    aspect = models.FloatField(null=True, blank=True)  # Aspect in degrees
    morphology = models.CharField(max_length=255, null=True, blank=True)  # Morphology description
    surface_type = models.CharField(max_length=255, null=True, blank=True)  # Surface type description
    vegetation = models.CharField(max_length=255, null=True, blank=True)  # Vegetation type description

    def __str__(self):
        return self.name


class Station(BaseModel):
    identifier = models.CharField(max_length=255, null=False, blank=False)
    name = models.CharField(max_length=255, null=True, blank=True)
    location_name = models.CharField(max_length=255, null=True, blank=True)
    location = models.ForeignKey(Location, on_delete=models.SET_NULL, related_name="stations", null=True, blank=True)
    commissioned_at = models.DateTimeField(null=True, blank=True)
    decommissioned_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.identifier}: {self.name}"


class RecordingDevice(BaseModel):
    station = models.ForeignKey(Station, on_delete=models.PROTECT, related_name="recording_devices", null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    type = models.CharField(max_length=255, null=True, blank=True)
    serial = models.CharField(max_length=255, null=True, blank=True)
    deveui = models.CharField(max_length=255, null=True, blank=True)
    commissioned_at = models.DateTimeField(null=True, blank=True)
    decommissioned_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.station.identifier} - {self.name} ({self.type})"


class Sensor(BaseModel):
    recording_device = models.ForeignKey(RecordingDevice, on_delete=models.CASCADE, related_name="sensors", null=True, blank=True)
    address = models.CharField(max_length=255, null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    type = models.CharField(max_length=255, null=True, blank=True)
    serial = models.CharField(max_length=255, null=True, blank=True)
    location = models.ForeignKey(Location, on_delete=models.CASCADE, related_name="sensors", null=True, blank=True)
    relative_z = models.FloatField(null=True, blank=True)  # relative to surface elevation
    commissioned_at = models.DateTimeField(null=True, blank=True)
    decommissioned_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.recording_device.name} - {self.name} ({self.type})"


class TimeSeries(BaseModel):
    name = models.CharField(max_length=255)
    sensor = models.ForeignKey(Sensor, on_delete=models.PROTECT, related_name="timeseries", null=False, blank=False)
    datatype = models.ForeignKey(DataType, on_delete=models.PROTECT, null=False, blank=False)
    # parameters defined as foreign key in the Parameter model

    def __str__(self):
        return f"{self.sensor.name} - {self.name} ({self.datatype.unit})"


class TimeSlice(models.Model):
    # time: TimescaleModels have a time field predefined, which is used as primary key
    # Moved away from TimescaleModel as forignkey from Measurement to TimescaleModel did not work
    time = models.DateTimeField()
    timeseries = models.ForeignKey(TimeSeries, on_delete=models.CASCADE, related_name="timeslices")
    # parameters defined as foreign key in the Parameter model

    def __str__(self):
        return f"{self.timeseries.name} - {self.timestamp}"
    

class Measurement(TimescaleModel):
    # time: TimescaleModels have a time field predefined, which is used as primary key
    seqnum = models.IntegerField(default=0)
    timeseries = models.ForeignKey(TimeSeries, on_delete=models.CASCADE, related_name="measurements")
    timeslice = models.ForeignKey(TimeSlice, on_delete=models.CASCADE, related_name="measurements", null=True, blank=True)
    datatype = models.ForeignKey(DataType, on_delete=models.CASCADE)
    value = models.FloatField(null=True, blank=True)
    stdev = models.FloatField(null=True, blank=True)
    masked = models.BooleanField(default=False)
    
    class Meta:
        # See https://forum.djangoproject.com/t/uniqueconstraint-or-unique-together/15569
        # See https://stackoverflow.com/a/77782458 (deprecated)
        # Create a unique constraint for the timeseries and time fields
        constraints = [
            models.UniqueConstraint(fields=['timeseries', 'time'], name='unique_measurement')
        ]

    def __str__(self):
        return f"{self.timeseries.name} - {self.time} - {self.value} {self.datatype.unit}"

class Parameter(BaseModel):
    name = models.CharField(max_length=255)
    timeseries = models.ForeignKey(TimeSeries, on_delete=models.CASCADE, related_name="parameters", null=True, blank=True)
    timeslice = models.ForeignKey(TimeSlice, on_delete=models.CASCADE, related_name="parameters", null=True, blank=True)
    datatype = models.ForeignKey(DataType, on_delete=models.CASCADE, related_name="parameters", null=True, blank=True)
    value_float = models.FloatField(null=True, blank=True)
    value_int = models.IntegerField(null=True, blank=True)
    value_str = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        # ensure either timeseries or timeslice is not null
        constraints = [
            models.CheckConstraint(check=models.Q(timeseries__isnull=False) | models.Q(timeslice__isnull=False), name="timeseries_or_timeslice_not_null")
        ]

    def __str__(self):
        # include the value of the value field that is not null
        outstr = f"{self.name}: {self.value_float or self.value_int or self.value_str}"
        if self.datatype:
            outstr += f" {self.datatype.unit}"
        return outstr
        

class SensorProperty(BaseModel):
    time = models.DateTimeField()
    name = models.CharField(max_length=255)
    sensor = models.ForeignKey(Sensor, on_delete=models.CASCADE, related_name="properties")
    timeslice = models.ForeignKey(TimeSlice, on_delete=models.CASCADE, related_name="sensor_properties", null=True, blank=True)
    datatype = models.ForeignKey(DataType, on_delete=models.CASCADE)
    value = models.FloatField()
    stdev = models.FloatField(null=True, blank=True)
    masked = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.sensor.name} - {self.name}: {self.value} {self.datatype.unit}"
