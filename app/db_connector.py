import logging
from datetime import datetime, timedelta, timezone

import asgiref.sync
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import settings

logger = logging.getLogger(__name__)


class EmptyQuerySet(Exception):
    pass


class RecordingDeviceNotFound(Exception):
    pass


class SensorNotFound(Exception):
    pass


class QueryTimeout(Exception):
    pass


def _build_session():
    retry = Retry(
        total=settings.backend_api_max_retries,
        read=settings.backend_api_max_retries,
        connect=settings.backend_api_max_retries,
        backoff_factor=settings.backend_api_backoff_factor,
        status_forcelist=[504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _api_get(path, params):
    base_url = settings.backend_api_base_url.rstrip("/")
    url = f"{base_url}/{path.lstrip('/')}"

    try:
        with _build_session() as session:
            response = session.get(url, params=params, timeout=settings.backend_api_timeout_seconds)
    except requests.Timeout as exc:
        raise QueryTimeout("Backend API request timed out") from exc
    except requests.RequestException as exc:
        raise RuntimeError(f"Backend API transport error: {exc}") from exc

    if response.status_code == 404:
        raise RecordingDeviceNotFound("RecordingDevice not found in backend API")
    if response.status_code == 504:
        raise QueryTimeout("Backend API request timed out")
    if response.status_code == 400:
        message = "Invalid backend API request"
        try:
            message = response.json().get("error", {}).get("message", message)
        except ValueError:
            pass
        raise ValueError(message)
    if response.status_code >= 500:
        raise RuntimeError(f"Backend API internal error ({response.status_code})")

    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError("Backend API returned invalid JSON") from exc


def _normalize_datetime_for_api(value, is_end=False):
    if value is None or value == "":
        return None

    # DatePickerRange commonly provides YYYY-MM-DD; map to an inclusive UTC interval.
    if isinstance(value, str) and len(value) == 10 and value.count("-") == 2:
        dt = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
        if is_end:
            dt = dt + timedelta(days=1) - timedelta(milliseconds=1)
        return dt.isoformat().replace("+00:00", "Z")

    parsed = pd.to_datetime(value, errors="raise", utc=False)
    if isinstance(parsed, pd.Timestamp):
        ts = parsed
    else:
        ts = pd.Timestamp(parsed)

    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")

    return ts.to_pydatetime().isoformat().replace("+00:00", "Z")


def _normalize_range_for_api(start, stop):
    end_iso = _normalize_datetime_for_api(stop, is_end=True)
    start_iso = _normalize_datetime_for_api(start, is_end=False)

    if end_iso is None:
        end_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if start_iso is None:
        start_iso = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat().replace("+00:00", "Z")

    return start_iso, end_iso


def _build_dataframe_from_measurements(payload):
    rows = payload.get("data", [])
    if not rows:
        raise EmptyQuerySet("No data found for the given query")

    frame = pd.DataFrame(rows)
    if "timestamp" not in frame.columns:
        raise RuntimeError("Backend API response missing timestamp column")

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame.set_index("timestamp", inplace=True)
    frame.sort_index(inplace=True)
    frame.drop_duplicates(inplace=True)
    return frame


def _to_timeseries_columns(frame):
    renamed = {}
    for col in frame.columns:
        if "__" in col:
            renamed[col] = col.split("__", 1)[1]
        else:
            renamed[col] = col

    frame = frame.rename(columns=renamed)
    # Preserve deterministic order while removing duplicated names.
    frame = frame.loc[:, ~frame.columns.duplicated()]
    return frame


def _get_metadata_payload():
    return _api_get("metadata", params={})


def _get_ground_temp_sensor_names(deveui):
    payload = _get_metadata_payload()
    sensor_names = [
        sensor.get("name")
        for sensor in payload.get("sensors", [])
        if sensor.get("recording_device_deveui") == deveui and sensor.get("name") and "groundtemp" in sensor["name"].lower()
    ]
    sensor_names = sorted(set(sensor_names))
    return sensor_names


def close_all_stale_db_connections():
    # Kept for backward compatibility; no-op on REST path.
    return None


def get_sensor_data(deveui, sensor_name, start, stop):
    close_all_stale_db_connections()
    start_iso, end_iso = _normalize_range_for_api(start, stop)

    payload = _api_get(
        "measurements",
        params={
            "deveui": deveui,
            "sensor_name": sensor_name,
            "start": start_iso,
            "end": end_iso,
        },
    )

    frame = _build_dataframe_from_measurements(payload)
    frame = _to_timeseries_columns(frame)
    return frame


def get_ground_temp(deveui, start, stop):
    close_all_stale_db_connections()
    start_iso, end_iso = _normalize_range_for_api(start, stop)

    sensor_names = _get_ground_temp_sensor_names(deveui)
    if not sensor_names:
        raise EmptyQuerySet("No data found for the given query")

    payload = _api_get(
        "measurements",
        params={
            "deveui": deveui,
            "sensor_name__in": ",".join(sensor_names),
            "start": start_iso,
            "end": end_iso,
        },
    )

    frame = _build_dataframe_from_measurements(payload)

    # Keep the old dataframe contract where columns are ground-temperature sensor names.
    ground_columns = [
        col for col in frame.columns if "__" in col and "groundtemp" in col.lower()
    ]
    if not ground_columns:
        raise EmptyQuerySet("No data found for the given query")

    renamed = {col: col.split("__", 1)[0] for col in ground_columns}
    frame = frame[ground_columns].rename(columns=renamed)
    frame = frame.loc[:, ~frame.columns.duplicated()]

    # Legacy return contract expected a datatype object; unit is unused by current app code.
    datatype = None
    return frame, datatype


def get_ground_temp_sensor_depths(deveui):
    close_all_stale_db_connections()

    payload = _get_metadata_payload()
    depths = {}
    for sensor in payload.get("sensors", []):
        if sensor.get("recording_device_deveui") != deveui:
            continue
        name = sensor.get("name")
        if not name or "groundtemp" not in name.lower():
            continue
        depths[name] = sensor.get("relative_z")

    if not depths:
        raise EmptyQuerySet("No data found for the given query")

    # Keep deterministic ordering from shallow to deep matching the old behavior.
    return dict(sorted(depths.items(), key=lambda item: item[1], reverse=True))


# Async wrappers for the frontend callback flow
get_sensor_data_async = asgiref.sync.sync_to_async(get_sensor_data, thread_sensitive=True)
get_ground_temp_async = asgiref.sync.sync_to_async(get_ground_temp, thread_sensitive=True)
get_ground_temp_sensor_depths_async = asgiref.sync.sync_to_async(get_ground_temp_sensor_depths, thread_sensitive=True)
