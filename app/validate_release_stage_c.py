from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import db_connector


MastToDeveui = {
    "Mast 6": "feedbeefcafe0002",
    "Mast 7": "feedbeefcafe0003",
    "Mast 9": "feedbeefcafe0001",
    "Mast 12": "feedbeefcafe0004",
    "Mast 13": "feedbeefcafe0005",
}


class _FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, status_code):
        self._status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def mount(self, *_args, **_kwargs):
        return None

    def get(self, *_args, **_kwargs):
        return _FakeResponse(self._status_code)


def _assert_error_mapping(sample_deveui):
    future_start = (datetime.now(timezone.utc) + timedelta(days=365)).isoformat().replace("+00:00", "Z")
    future_end = (datetime.now(timezone.utc) + timedelta(days=366)).isoformat().replace("+00:00", "Z")

    try:
        db_connector.get_sensor_data(sample_deveui, "BatteryVoltage", future_start, future_end)
        raise AssertionError("Expected EmptyQuerySet for empty-range query")
    except db_connector.EmptyQuerySet:
        pass

    try:
        db_connector.get_sensor_data("invalid-deveui-for-release-c-validation", "BatteryVoltage", future_start, future_end)
        raise AssertionError("Expected RecordingDeviceNotFound for unknown deveui")
    except db_connector.RecordingDeviceNotFound:
        pass

    with patch("db_connector._build_session", return_value=_FakeSession(504)):
        try:
            db_connector.get_sensor_data(sample_deveui, "BatteryVoltage", future_start, future_end)
            raise AssertionError("Expected QueryTimeout for 504 response")
        except db_connector.QueryTimeout:
            pass

    with patch("db_connector._build_session", return_value=_FakeSession(500)):
        try:
            db_connector.get_sensor_data(sample_deveui, "BatteryVoltage", future_start, future_end)
            raise AssertionError("Expected RuntimeError for 500 response")
        except RuntimeError:
            pass


def _assert_data_contracts(sample_deveui, start, end):
    weather_df = db_connector.get_sensor_data(sample_deveui, "Weather Sensor", start, end)
    expected_weather_columns = {"AirTemp", "RelHum", "BarometricPressure"}
    missing = expected_weather_columns - set(weather_df.columns)
    if missing:
        raise AssertionError(f"Weather contract missing expected columns: {sorted(missing)}")

    _incl_df = db_connector.get_sensor_data(sample_deveui, "Inclination Sensor", start, end)
    _battery_df = db_connector.get_sensor_data(sample_deveui, "BatteryVoltage", start, end)

    ground_df, _datatype = db_connector.get_ground_temp(sample_deveui, start, end)
    depth_map = db_connector.get_ground_temp_sensor_depths(sample_deveui)

    for column in ground_df.columns:
        if column not in depth_map:
            raise AssertionError(f"Ground temp depth metadata missing for column {column}")


def _find_representative_sample():
    end = datetime.now(timezone.utc)
    # Use a broad range so local/dev restores with sparse data still validate.
    start = end - timedelta(days=3650)

    start_iso = start.isoformat().replace("+00:00", "Z")
    end_iso = end.isoformat().replace("+00:00", "Z")

    for _mast, deveui in MastToDeveui.items():
        try:
            weather_df = db_connector.get_sensor_data(deveui, "Weather Sensor", start_iso, end_iso)
            _ = db_connector.get_sensor_data(deveui, "Inclination Sensor", start_iso, end_iso)
            _ = db_connector.get_sensor_data(deveui, "BatteryVoltage", start_iso, end_iso)
            ground_df, _ = db_connector.get_ground_temp(deveui, start_iso, end_iso)
            depth_map = db_connector.get_ground_temp_sensor_depths(deveui)

            required_weather = {"AirTemp", "RelHum", "BarometricPressure"}
            if not required_weather.issubset(set(weather_df.columns)):
                continue
            if not len(ground_df.columns):
                continue
            if any(column not in depth_map for column in ground_df.columns):
                continue

            return deveui, start_iso, end_iso
        except (db_connector.EmptyQuerySet, db_connector.RecordingDeviceNotFound, db_connector.QueryTimeout, Exception):
            continue

    raise AssertionError(
        "Could not find a representative mast/deveui with weather, inclination, battery, and ground-temperature data in local environment"
    )


def main():
    stage_name = "release-stage-c"

    try:
        sample_deveui, start_iso, end_iso = _find_representative_sample()

        _assert_data_contracts(sample_deveui, start_iso, end_iso)
        _assert_error_mapping(sample_deveui)
    except Exception as exc:
        print(f"FAIL: {stage_name} - {exc}")
        raise SystemExit(1)

    print(f"PASS: {stage_name}")
    raise SystemExit(0)


if __name__ == "__main__":
    main()
