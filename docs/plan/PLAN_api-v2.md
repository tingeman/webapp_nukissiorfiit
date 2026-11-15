# Plan: API v2 refactoring (phase 1: v2 data access and logger selector; phase 2: TSP-style page)

Status: **phase 1 approved 2026-11-15; phase 2 approved 2026-11-15**. Roadmap: R1, R2 (phase 1); R5 (phase 2). Branch: `agent-api-v2` (from `develop/api_v2/main`).

## Goal

Phase 1 (as few changes to the page as possible):

1. The app reads only through the monitoring_db API v2 (`/api/v2/measurements/raw`, `/api/v2/metadata`), plus `/api/v1/metadata` for the station list (see Design notes).
2. R1: no retries of HTTP 504; a timeout shows its message at once.
3. R2: one request set per dashboard update instead of 5 sequential v1 requests, with each request bounded (1-year chunks, at most 2 in parallel, gzip).
4. Masts and their loggers come from the API metadata, not from hard-coded DevEUIs. A logger dropdown selects the Tekbox (2024-09 to 2026-09) or the Campbell CR350 (from 2026-09) of a mast.
5. Inclination works for both sensor types (Tekbox TBSCS1: RollAngle/PitchAngle; Campbell SOLAR-2-45: XAngle/YAngle).

Phase 2 (a new page at `/app/nuki/v2/`, the future reference design; phase 1 is taken down after the switch-over):

6. TSP code structure (layout, callbacks, figures, texts) with its own API client in `app/v2/`, combining the best of TSP's `Client` and phase 1's `db_connector` (planned 2026-11-15).
7. Daily view by default; Full (raw, thinned, refetched on zoom) for at most 1 year at a time.
8. All sensor panels in one figure with linked x axes; a ground temperature profile with a day slider and a choice of envelope (day min/max, annual envelope, or both).
9. The latest logger by default; "All loggers" of a mast on request.
10. An Excel download with its own selectors (years, loggers or all), with a Full and a Daily (mean/min/max) sheet per logger.

## Current state

- `app/app.py:41-56`: masts and DevEUIs hard-coded. **Mast 13 maps to `feedbeefcafe0005`, which is the DTU test device** ("Testing at DTU in Lyngby"). The real M13 Tekbox is `feedbeefcafe0008` (data not yet in the database, monitoring_db roadmap); its Campbell is serial 12216.
- `app/app.py:118-282`: one callback makes 4 data calls, one after the other: battery, ground temperature (+ depths), "Weather Sensor", "Inclination Sensor". Each one has its own copy of the error handling (4 x 4 `except` blocks).
- `app/app.py:198`: pressure is labelled kPa. monitoring_db now stores BarometricPressure in **Pa** (Tekbox parser `scale: 100`), so the plot shows values around 100 000 "kPa". The axis stays kPa and the values are converted (user decision 2026-11-15).
- `app/app.py:231`: the inclination plot draws every column of "Inclination Sensor": Roll, Pitch, **CompassHeading** (0-360 deg, squashes the angle axis) and, since the Tekbox payload work, **SensorTemp** (°C on a degree axis). The Campbell sensor is called "Dual Axis Inclinometer", so it is not found at all.
- `app/db_connector.py:31-45`: retries 504 (R1). `:147-204`: v1 `/metadata` (whole database, no narrowing) is fetched **twice** per update (`_get_ground_temp_sensor_names`, `get_ground_temp_sensor_depths`), plus 3 `/measurements` requests for the whole range in one request each: 5 sequential requests per update.
- `app/db_connector.py:241-245`: `sync_to_async(..., thread_sensitive=True)`, a leftover from the Django ORM. It runs every call of a process in one shared thread.
- `app/config.py:13-22`: `BACKEND_API_BASE_URL` includes `/api/v1`; the InfluxDB fields are required although they are unused.
- `tests/fake_api.py`: v1 only (`/api/v1/metadata`, `/api/v1/measurements`); fixtures `tests/fixtures/metadata.json`, `measurements.json` (one station `MAST06`, old sensor types).

What the database holds (dev definitions `00_assets/mdb_dev_ingestion_data/dev-definitions/station_defs/`, 2026-11-14):

| Mast | Station(s) (`name`) | Tekbox (deveui, period) | Campbell CR350 (serial, from) |
|---|---|---|---|
| 6 | SIS2024-01 | 0002, 2024-09-11 to 2026-09-13 | 12096, 2026-09-14 |
| 7 | SIS2024-02 | 0003 | 12217 |
| 9 | SIS2024-03 (Tekbox, ends 2026-09-18) and **NUKI2026-M9** (Campbell, new string about 2 m away) | 0001 (data stop 2024-10-18) | 12215, 2026-09-19 |
| 12 | SIS2024-04 | 0004 | 12094 (not transmitting, parser staged) |
| 13 | SIS2024-05 | 0008, 2025-04-08 to 2026-09-26 (no data in the database yet) | 12216, 2026-09-26 (live; battery, weather, inclinometer, 12-node string) |

Sensors per logger: `BatteryVoltage`; `Weather Sensor` (AirTemp, RelHum, BarometricPressure in Pa); `GroundTemp01..12` (series `GroundTemperature`, `relative_z` < 0); inclination: Tekbox `Inclination Sensor` (TBSCS1: RollAngle, PitchAngle [Angle], CompassHeading [Heading], SensorTemp), Campbell `Dual Axis Inclinometer` (SOLAR-2-45: XAngle, YAngle [Angle], SensorTemp). Both stations of Mast 9 are named "Nukissiorfiit Mast 9".

## Design notes

### Data access (T2), modelled on `webapp_grl_tsp/app/api_client.py`

- **Base URL:** `BACKEND_API_BASE_URL` stays. A trailing `/api/v1` (or `/api/v2`) is stripped with one WARNING at start, so the production env file (`http://192.38.87.126/mdb/api/v1`) keeps working. No deployment change. The template shows the root form `http://<data02-host>/mdb`.
- **Retries (R1):** `Retry(connect=BACKEND_API_MAX_RETRIES, read=0, status=0)`, no `status_forcelist`. A 504 (`QUERY_TIMEOUT`) or a read timeout raises `QueryTimeout` at once.
- **Station list:** `GET /api/v1/metadata` (the whole database, as today and as the TSP app does; no time ranges, so cheap), cached per process for 10 minutes behind a lock; failures are not cached. From it, a **mast catalogue**, which holds only the Nukissiorfiit stations, never the whole database. A station is included when its identifier fully matches `NUKI_STATION_PATTERN` (default `^(SIS2024-0[1-5]|NUKI202[0-9]-.+)$`, user decision 2026-11-15): the five SIS2024 masts, plus any NUKI2020 to NUKI2029 station without a code change. Another SIS2024-NN station elsewhere in Sisimiut does not match. An invalid regex stops the app at start with a clear message. The included stations are grouped by station `name` ("Nukissiorfiit Mast 9" covers SIS2024-03 and NUKI2026-M9). Mast label: the name without the "Nukissiorfiit " prefix, sorted by mast number. Per mast, the loggers: value `serial:<station>:<serial>` or `deveui:<deveui>` (the TSP rule: the API cannot combine deveui with station), label `<type> (<commissioned> to <decommissioned or now>)`.
- **Logger periods:** `GET /api/v2/metadata?station=<identifier>` for the stations of the selected mast (1 or 2 small requests), cached 10 minutes per station. `first/last_measurement_time` of each logger set the date picker's default range and limits. v2 metadata without `station` would compute the time ranges over the whole database (read_api.md), so it is always narrowed.
- **Measurements:** one `GET /api/v2/measurements/raw` request set per update for the selected logger, **without sensor filters**: all ~20 columns (battery, weather, 12 ground temperatures, inclination) arrive together, and the figures are built from that one frame. The range is cut into 1-year chunks (`chunks()` from the TSP client, `BACKEND_API_RAW_CHUNK_YEARS`, default 1); at most `BACKEND_API_MAX_PARALLEL` (default 2) in flight, with a thread pool created per call (gunicorn preload/fork). A logger lives at most about 2 years, so a request set is 1 to 3 requests. `requests` sends `Accept-Encoding: gzip` by default; a test checks it.
- **Columns:** the payloads become one DataFrame (UTC index, columns = API key `<sensor>__<series>`), plus a dict of column details (sensor name/type, series name, datatype, unit, `relative_z`). Selection is by metadata, not by name parsing:
  - battery: series `BatteryVoltage`;
  - ground temperature: series `GroundTemperature` and `relative_z <= 0`, shallow to deep, label `-0.10 m` (today's labels);
  - weather: series `AirTemp`, `RelHum`, `BarometricPressure` (plotted in **kPa**, as today; user decision 2026-11-15). The connector converts it before plotting, using the column's unit: Pa / 1000, hPa / 10, kPa as is. An unknown unit is plotted unconverted, with its own unit in the axis label, and a WARNING is logged;
  - inclination: **datatype `Angle`** (user decision 2026-11-15). Tekbox gives Roll/Pitch, Campbell X/Y; the legend shows the series name. CompassHeading (datatype Heading) and SensorTemp are not plotted in phase 1 (phase 2 gives them their own panels). Any future inclinometer works without a code change.
- **Async:** callbacks stay `async`; the wrappers become `sync_to_async(..., thread_sensitive=False)` (there is no ORM any more).

Effect on the API, per dashboard update:

| | Today (v1) | Phase 1 (v2) |
|---|---|---|
| Metadata | 2 x whole-database `/v1/metadata`, every update | `/v1/metadata` once per 10 min per worker; `/v2/metadata?station=` once per included station (6) per 10 min per worker, sequential, when the mast list is built (T2: needed to leave out loggers and masts without data) |
| Measurements | 3 sequential `/v1/measurements`, whole range each (JSON rows built in Python) | 1 to 3 `/v2/.../raw` chunks of at most 1 year, at most 2 in parallel, columnar, gzip |
| Worst case in parallel per app worker | 1 | 2 |

Always raw (user decision 2026-11-15): a 2-year range of one logger is about 17 000 timestamps x 20 columns; gzip makes that about 1 MB on the wire. The plots keep `lines+markers` with all points, as today. Thinning and `/daily` come in phase 2 (T8).

### Page (T3), minimal change

- Mast dropdown: options from the catalogue (the hard-coded list goes). Logger dropdown (new) under it: options for the selected mast, default the logger with the latest `last_measurement_time`. Loggers without measurements (`last_measurement_time` null) are left out: Mast 13 shows only its CR350 (12216, live since 2026-09-26) until its Tekbox (0008) data is ingested, and then shows both without a code change.
- When the logger changes, the date range is set to the last 30 days of that logger's data (today: the last month before today, which would be empty for a Tekbox). The picker's `min/max_date_allowed` come from the logger's first/last measurement. The user can still change the range afterwards.
- One callback fetches the frame and builds the same 6 graphs from it, with one error handler (the messages stay those of today: timeout, no data, logger not found, unknown error).
- Titles name the mast and logger (`Ground Temperature - Mast 6 (CR350 12096)`).
- The table keeps showing the ground temperatures (phase 2 drops the table for an Excel download, T12).

### Phase 2 design (planning session 2026-11-15)

**Side by side with phase 1** (decided 2026-11-15): a second `Dash` instance on the same Flask `server`, in `app/v2/`, at `/app/nuki/v2/` (T7, done). Phase 1 stays at `/app/nuki/` unchanged, and each page links to the other. No deployment change: nginx's `location /app/nuki/` already proxies the sub-path, and the port, healthcheck (`/app/nuki/`) and env are unchanged. **Phase 2 is a free redesign** and becomes the reference design. The phase 1 page and `db_connector.py` are frozen (bug fixes only) and are removed after the switch-over (T15). Phase 1's tests keep running until then.

**API client `app/v2/api_client.py`** (decided 2026-11-15: its own client, the best of both). Phase 1 code is moved over where it is right, not rewritten; `app/v2/` does not import `db_connector.py`.

| Part | Taken from | Why |
|---|---|---|
| `Client` class built from settings (`from_settings`); only plain attributes and a lock at construction; sessions and pools per call | TSP | testable without globals; safe with gunicorn preload/fork |
| One `ApiError` carrying the page text (`texts.py`) and the technical reason; one WARNING per failure | TSP | callbacks show `str(error)`; no exception-to-message mapping per callback (phase 1's `EmptyQuerySet`, `RecordingDeviceNotFound` are not carried over) |
| `daily()` and `raw()` sharing `_fetch` (at most `max_parallel` in flight, queued chunks dropped after an error), answer shape checks, `RequestTiming` per request | TSP | needed for Daily/Full and the info line |
| Chunks: daily split at 1 August in `BACKEND_API_DAILY_CHUNK_YEARS` (default 3) hydrological years; raw in `BACKEND_API_RAW_CHUNK_YEARS` (default 1) from the start; datetimes for zoom windows | TSP | same rule as the reference app |
| Root URL normalisation (`/api/v1`, `/api/v2` suffix stripped, one WARNING) | db_connector | the production env file keeps working |
| Connect-only retries (R1: no read or status retries; a 504 or read timeout fails at once) | db_connector | TSP has no retries; a refused connection during a backend restart is safe to retry |
| Mast catalogue (`NUKI_STATION_PATTERN`, grouping by station `name`, v2 metadata per station cached 10 min, loggers and masts without data left out, labels and order) | db_connector | Nuki-specific, already tested |
| Column selection by column details (series name, datatype `Angle`/`Heading`, `relative_z`); pressure converted to kPa by unit | db_connector | independent of sensor names; new inclinometers work without a code change |
| Data shape: per column a details dict plus numpy arrays (raw: time/value; daily: time/mean/min/max/count) | TSP (`rawseries.py`, `trumpet.py`, ported) | thinning and the profile work on arrays; no wide pandas frame per update |

Callbacks are plain sync functions, as in TSP (gunicorn runs sync workers, so `sync_to_async` adds nothing). The two clients keep separate metadata caches until the switch-over: per worker and 10 minutes, 1 + 6 small metadata requests each, 14 in all (accepted 2026-11-15).

**Page.** Selectors: mast, logger (default the logger with the latest data; "All loggers" as the last option), date range (limits from the logger's first/last measurement, default its whole period), Daily/Full switch. `dcc.Loading` around the figures, an info line (requests, seconds, size, as in TSP), error texts from `texts.py`. Styling: Bootstrap 5.3.8 **bundled** in `app/v2/assets/` (copied from webapp_grl_tsp with its sha256; decided 2026-11-15: no visitor IPs go to a CDN, works without the CDN, fixed in the tested image) plus `style.css`.

- **Daily** (default): one `/daily` request set for the range (1 request for a logger's whole life); mean lines with a min/max band.
- **Full**: raw for at most **366 days** (decided 2026-11-15). A longer range loads its last 366 days and says so in the info line. Thinned with `rawseries.thin` (about 2000 points per series); a zoom (`relayoutData`) refetches raw for the visible window only, so any window keeps its full resolution.
- **One figure** (`make_subplots`, `shared_xaxes`): ground temperature (one line per depth), air temperature, relative humidity, pressure (kPa), one row per inclination angle (own scale), heading, sensor temperature, battery. Rows without data are left out.
- **Profile**: temperature against depth for one day, from the daily data already loaded, with a day slider over the loaded days. Envelope selector (decided 2026-11-15: both, the user chooses): "Day min/max" (band of the chosen day, default), "Annual envelope" (min/max of the daily means over the 365 days up to the chosen day, TSP's trumpet), "Both". In Full mode one extra `/daily` request set for the range feeds the profile.
- **All loggers** (on request; decided 2026-11-15): the mast's loggers are fetched one after the other (1 daily request each); series are joined by depth (ground temperature) or series name, with a vertical marker at each logger change. Mast 9's two stations are different strings at different places, so their series stay apart (separate legend groups).

**Download** (decided 2026-11-15): a panel with its own selectors: from year / to year (calendar years, default the first and last year with data of the selected loggers), the mast's loggers (checklist with "All", default all), and a button. The file is an **Excel workbook** (`openpyxl`, new hash-locked dependency): an `Info` sheet (mast, loggers, range, download time, column details: sensor, series, unit, depth), then per logger a `<logger> full` sheet (every raw measurement, one column per series) and a `<logger> daily` sheet (mean, min, max and count per series, from `/daily`). Not zipped: an .xlsx is already a zip archive. Sheet names at most 31 characters. Built in memory and sent with `dcc.Download`; the button is disabled while a download runs.

Effect on the API, per action:

| Action | Requests | Range per request | In parallel |
|---|---|---|---|
| Page load / selection (Daily) | 1 daily per logger (1, or 2 for All loggers, one after the other) | up to 3 years | 1 |
| Full | 1 raw (366 days at most) + 1 daily for the profile | 1 year | at most 2 |
| Zoom in Full | 1 raw | the visible window | 1 |
| Download, one logger | up to 3 raw + 1 daily | 1 year raw | at most 2 |
| Download, All (2 loggers) | up to 2 x (3 raw + 1 daily), logger after logger | 1 year raw | at most 2 |

The default view costs less than phase 1 (one small daily request instead of 1 to 3 raw chunks). The download is the heaviest action (about 0.5 MB on the wire per logger-year), bounded by the logger lives (about 2 years each) and by `BACKEND_API_MAX_PARALLEL`.

**Time limits.** API statement timeout 20 s per query (504), `BACKEND_API_TIMEOUT_SECONDS` 30 s per request, gunicorn `--timeout 120`, and nginx `location /app/nuki/`: `proxy_read_timeout 120s` (dtu_sus_web_server `agent-fixes` `396cc0a`, 2026-11-15, not yet deployed; until then nginx's default of 60 s applies). Page actions (at most 2 requests) stay well below. A download can make up to 8 requests in 4 rounds, so it gets a **time budget** (`DOWNLOAD_TIME_BUDGET_S`, default **75 s**): the client's `_fetch` takes an optional deadline and starts no new chunk after it; the download then fails with "Download too large; choose fewer years or loggers", so no API work runs on after nginx has given up. A chunk started just before the deadline can still run for `BACKEND_API_TIMEOUT_SECONDS` (30 s), and the workbook takes a few seconds, so budget + 30 s + build must stay below 120 s. With nginx at 60 s the budget would have to be about 25 s, so the nginx change must be deployed before phase 2 is released (T14). Dash background callbacks (job queue + disk cache) were considered and left out: a new dependency and shared state for downloads of a few MB.

## Assumptions

- Branch `agent-api-v2` from `develop/api_v2/main` (the branch you are on).
- Stations are filtered by identifier pattern (decided), and masts are grouped by station `name` (assumed). If monitoring_db later gets a site/group field, the catalogue switches to it (see the DB notes below).
- `calibrated` and `include_masked` are not sent (API defaults: calibrated values, masked values left out), as in TSP. No calibrations are registered for these stations yet, so the values do not change.
- The InfluxDB settings stay required in phase 1. Their removal goes with the legacy cleanup (R3) as a separate change.
- gunicorn stays at 4 sync workers. Threaded workers are deferred (R4): with 2 parallel requests per callback, 4 workers x 4 threads could put 32 requests on the API's 4 workers.
- No local Python: all tests run in Docker (`tests/run.sh`). Ruff, mypy and pytest are not set up.
- Phase 2: the page has no data table; the Excel download replaces it. The download's "loggers" are those of the selected mast; "All" means all loggers of that mast, not all masts. Years are calendar years (UTC). The daily sheet comes from the API's `/daily` (mean, min, max, count), not computed from raw: page and file agree, one place aggregates; it differs from raw only on days where several timeseries share a column key (read_api.md), which no Nuki logger has today. Downloads are not cached.
- Phase 2: the date picker keeps no session persistence (the logger sets the range); the Daily/Full choice and the envelope choice persist per browser session (`persistence_type="session"`).
- Phase 2: the daily data comes from the API's `/daily` (the same numbers as the page), not computed from raw in the app.

## Tasks

### T1. Fake API v2 and fixtures
- Files: `tests/fake_api.py`, `tests/fixtures/` (new `metadata.json` content, `measurements.json` per logger), `tests/run.sh` (list of files copied, if names change).
- Acceptance: the stub answers `/api/v1/metadata`, `/api/v2/metadata` (with `station`, first/last times), `/api/v2/measurements/raw` (selectors `deveui` or `station`+`serial`, inclusive `start`/`end`, columnar shape and column details as in read_api.md, the 400/404/504 error shapes; `server.force` and `server.requests` work as now). Fixtures: Mast 6 (one station, Tekbox deveui + Campbell serial, Tekbox inclination with Roll/Pitch/Heading/SensorTemp, Campbell X/Y/SensorTemp), Mast 9 (SIS2024-03 Tekbox + NUKI2026-M9 Campbell), Mast 13 (Campbell with data, Tekbox without measurements, which the catalogue must leave out), two stations that must be filtered out (a non-Nuki station with ground temperatures, and a hypothetical `SIS2024-07` that the default pattern must not match), pressure in Pa. The v1 `/measurements` endpoint stays until T2 no longer uses it.
- Validation: `bash -n tests/run.sh`; full `bash tests/run.sh webapp_nuki:local` still passes (the v1 tests are unchanged).
- Agent: implementer
- Deployment impact: none

### T2. db_connector on API v2
- Files: `app/db_connector.py`, `app/config.py`, `tests/unit/test_db_connector.py` (rewritten for v2), `tests/unit/support.py`.
- Acceptance: as in Design notes "Data access": base URL normalisation; no 504 retry (unit test: one request on 504, `QueryTimeout`); connect retries only; v1 metadata and v2 per-station metadata cached 10 min (unit test: second call makes no request; a failure is not cached); `mast_catalogue()` (unit tests: only the pattern's stations are included, Mast 9 groups two stations, a custom pattern from env applies, an invalid pattern fails at config load); `logger_periods(mast)`; `get_logger_data(logger, start, end)` returning frame + column details, chunked at 1 year, at most `BACKEND_API_MAX_PARALLEL` at once (unit test with a 2.5-year range: 3 requests, contiguous non-overlapping bounds, results in time order); selection helpers `battery`, `ground_temperature` (with depth labels), `weather` (pressure converted to kPa by unit; unit tests for Pa, hPa, kPa and an unknown unit), `inclination` (datatype Angle). The `Accept-Encoding` header contains `gzip`. Async wrappers with `thread_sensitive=False`. The v1 measurement functions are removed; `EmptyQuerySet`, `QueryTimeout`, `RecordingDeviceNotFound` stay for app.py.
- Validation: `TEST_FILTER='^unit:' APP_SRC=app bash tests/run.sh webapp_nuki:local`
- Agent: implementer
- Deployment impact: none (same env variable names; new optional variables `BACKEND_API_MAX_PARALLEL`, `BACKEND_API_RAW_CHUNK_YEARS`, `NUKI_STATION_PATTERN`, with defaults)

### T3. Page: mast and logger dropdowns, one fetch per update
- Files: `app/app.py`, `tests/smoke_check.py` (the callback smoke test uses the new inputs).
- Acceptance: as in Design notes "Page": mast options from the catalogue; logger dropdown with the default latest logger; the date range follows the logger; one data call per update; the 6 graphs and the table as today, with the inclination plot showing only Angle series for both sensor types, pressure in kPa (label "Pressure (kPa)" as today, values about 100), and titles with mast and logger; one error handler; no hard-coded DevEUIs remain (`grep -n feedbeef app/app.py` finds nothing).
- Validation: full `bash tests/run.sh webapp_nuki:local` (smoke tests run the callback against the stub for a Tekbox and a Campbell logger).
- Agent: implementer
- Deployment impact: none

### T4. Env template and docs
- Files: `webapp_nuki.env.example`, `README.md` (TODO list: mark what phase 1 covers; Notes: v2), `CROSS_REPO_DEV_README.md` (the env TODO points to the template).
- Acceptance: the template documents the root URL form, the stripped-suffix behaviour, that `BACKEND_API_MAX_RETRIES` now covers connection errors only, and the new optional variables with their defaults.
- Validation: review; unit tests (config defaults).
- Agent: simple-task
- Deployment impact: none required. At the next edit of the production env file, `BACKEND_API_BASE_URL` can move to the root form (item for dtu_sus_web_server, listed in the report).

### T5. Check against the local backend (you)
- With the dev database restored and the backend up (`CROSS_REPO_DEV_README.md`):
  `docker compose --project-name nuki -f compose.develop.yml -f compose.develop.local.yml up -d --build webapp_nuki`, then `http://localhost:8050/app/nuki/`.
- Check: 5 masts listed (Mast 9 with 2 loggers on 2 stations, Mast 13 without the test device); for each logger the default range has data; inclination shows Roll/Pitch (Tekbox) and X/Y (Campbell); pressure about 100 kPa; the backend log shows 1-3 `/v2/measurements/raw` requests per update and no repeats after a timeout. Note the response times for a full logger period in the status file.

### T6. Release phase 1 (you)
- Fast-forward `develop/main` to the approved branch (with your permission), tag, then in dtu_sus_data02: `scripts/build_image.sh webapp_nuki https://github.com/tingeman/webapp_nukissiorfiit.git <tag>`; set the version in `dtu_sus_web_server/ansible/vars/releases.yml` and deploy with its playbook.

### Phase 2 tasks (planned 2026-11-15, branch `agent-api-v2-phase2`)

Phase 1 is released on its own (T6). Phase 2 is developed on `agent-api-v2-phase2` (from `agent-api-v2`; merge `agent-api-v2` into it when phase 1 changes) and released later (T14), when you decide.

### T7. Page skeleton at `/app/nuki/v2/` (done)
- Second Dash app in `app/v2/`, links both ways (`fd42b1d`).

### T8. API client v2 and the stub's `/daily`
- Files: `app/v2/api_client.py`, `app/v2/rawseries.py` and `app/v2/trumpet.py` (ported from webapp_grl_tsp), `app/v2/texts.py` (error texts), `app/config.py` (`BACKEND_API_DAILY_CHUNK_YEARS`, default 3), `tests/fake_api.py` and `tests/fixtures/make_fixtures.py` (`/api/v2/measurements/daily`: per UTC day mean/min/max/count, same selectors and errors as `/raw`), `tests/unit/test_v2_api_client.py`, `tests/run.sh` (file list, if needed).
- Acceptance: as in "Phase 2 design", API client table. Unit tests: root URL normalisation; connect retry, but one request only on a 504 or read timeout (`ApiError` with the timeout text); metadata caches (second call makes no request, failures not cached); catalogue (pattern, Mast 9 grouping, Mast 13 without its empty Tekbox, non-Nuki stations left out); daily chunks at 1 August, raw chunks from the start, datetimes for a zoom window; at most `max_parallel` in flight; `Accept-Encoding` contains gzip; selections (ground temperature by depth, inclination by `Angle`, heading by `Heading`, pressure in kPa for Pa/hPa/kPa, an unknown unit unconverted with a WARNING); thinning keeps each bucket's min and max; `_fetch` with a deadline starts no chunk after it. `app/v2/` does not import `db_connector`. Phase 1 code and tests unchanged.
- Validation: `TEST_FILTER='^unit:' APP_SRC=app bash tests/run.sh webapp_nuki:local`, then the full suite (tests/ changed).
- Agent: implementer
- Deployment impact: none (new optional variable with a default).

### T9. Page: selectors, Daily/Full, one linked figure
- Files: `app/v2/layout.py`, `callbacks.py`, `figures.py`, `texts.py`, `app/v2/assets/bootstrap.min.css` (copied from webapp_grl_tsp, same sha256) and `style.css`, `README.md` (third-party file note as in TSP), `tests/unit/test_v2_page.py`, `tests/smoke_check.py`.
- Acceptance: as in "Phase 2 design", Page, without the profile, All loggers and the download: mast and logger selectors from the catalogue, default the latest logger; date limits and default range from the logger period; Daily default with min/max bands; Full limited to 366 days (a longer range loads its last 366 days, with a note), thinned, refetched on zoom; one `make_subplots` figure with shared x axes and the rows listed there; `dcc.Loading`; info line; errors shown from `ApiError`. Unit tests count the stub's requests per action (Daily: 1; Full: 1 raw; zoom: 1 raw for the window). The page loads no CDN resource.
- Validation: full `bash tests/run.sh webapp_nuki:local`.
- Agent: implementer
- Deployment impact: none

### T10. Ground temperature profile with day slider and envelope choice
- Files: `app/v2/layout.py`, `callbacks.py`, `figures.py`, `texts.py`, tests.
- Acceptance: profile of the chosen day (daily mean against depth); slider over the loaded days (default the last day); envelope selector "Day min/max" (default), "Annual envelope", "Both"; moving the slider or changing the envelope makes no API request (test); in Full mode the profile uses one extra daily request set for the range.
- Validation: unit tests, full suite.
- Agent: implementer
- Deployment impact: none

### T11. "All loggers" view
- Files: `app/v2/callbacks.py`, `figures.py`, `api_client.py` (if a join helper fits there), tests.
- Acceptance: as in "Phase 2 design", All loggers: loggers fetched one after the other (test: never two daily requests in flight), joined by depth or series name, a marker at each logger change, Mast 9's two stations kept apart. Full with All loggers: raw for the 366-day window from each logger that has data in it.
- Validation: unit tests, full suite.
- Agent: implementer
- Deployment impact: none

### T12. Excel download
- Files: `app/requirements.in` (`openpyxl`), the lock files regenerated with `python scripts/compile_lock.py`, `app/v2/download.py` (workbook builder, no Dash), `layout.py`, `callbacks.py`, `texts.py`, tests.
- Acceptance: as in "Phase 2 design", Download: year range (default all years with data), logger checklist with "All" (default all), button disabled while running; `Info` sheet, then `<logger> full` and `<logger> daily` per logger, names at most 31 characters; requests as in the API table (test: logger after logger, at most `max_parallel` in flight). A test opens the workbook with openpyxl and checks sheet names, headers, row counts against the fixtures, and that the daily mean/min/max/count match the stub; a download over the time budget stops starting chunks and shows the "too large" text (test with a slow stub).
- Validation: rebuild the image (requirements changed), full `bash tests/run.sh webapp_nuki:local`.
- Agent: implementer
- Deployment impact: none (the image grows slightly)

### T13. Docs and env
- Files: `webapp_nuki.env.example` (`BACKEND_API_DAILY_CHUNK_YEARS`, `DOWNLOAD_TIME_BUDGET_S`), `README.md` (the v2 page, its modules, the download; TODO list updated).
- Validation: review, unit tests.
- Agent: simple-task
- Deployment impact: none

### T14. Release phase 2 alongside phase 1 (you)
- Before: merge dtu_sus_web_server `agent-fixes` (`396cc0a`, nginx `proxy_read_timeout 120s` for `/app/nuki/`) into its `develop/main` and deploy nginx (`playbook_install_nginx.yml`); check with `curl -sI https://arctic.sustain.dtu.dk/app/nuki/` that the page still answers.
- As T6, with the tag on `develop/main` after `agent-api-v2-phase2` is merged (with your permission). Then check `/app/nuki/v2/` on the server: Daily load times, a Full zoom, an All-loggers download for Mast 6, and the API log for the request counts in the table above.

### T15. Switch-over (only after your decision)
- The new page moves to `/app/nuki/`; the classic page is removed together with `db_connector.py` and their tests, in its own commit (the module that creates the Flask server stays). Check that the healthcheck path `/app/nuki/` still answers. Agent: implementer.
- Deployment impact: none if the path stays `/app/nuki/`; otherwise nginx and the healthcheck in dtu_sus_web_server.

## Notes on the sensor definitions in monitoring_db (for you; no change in this plan)

The app works with the definitions as they are. Suggestions, in order of value:

1. **A site/group for masts.** Mast 9 is two stations (SIS2024-03, NUKI2026-M9), tied together only by the same `name`. A separate station is right (new string, new location), but a `site` or `group` field on Station, served in the metadata, would let the app group masts without relying on the name.
2. **Inclination axes.** The Tekbox's Roll/Pitch and the Campbell's X/Y are different instruments and keep their names (they may not be the same axes). It would help to record the mounting orientation (which axis points along the line) in the sensor's notes, so that the two periods can be compared. Rename them only if they are known to be the same physical axes. The app relies on datatype `Angle` vs `Heading`, which the parsers already set correctly.
3. **`relative_z` of BatteryVoltage** is 4.00 m on SIS2024-0N and 1.00 m on NUKI2026-M9. It has no physical meaning for a battery; consistent values (or null) would be cleaner. The app does not use it.
4. **Open data items** (already on the monitoring_db roadmap): M12 Campbell has a placeholder serial and is not transmitting; M13 Tekbox data is not ingested yet; the Mast 9 Tekbox data stops on 2024-10-18.
5. **Station location in the metadata.** The map insert (README TODO, R6) needs `lat/lon` in `/metadata` stations. Today the definitions have them, but the API does not serve them.

## Out of scope

- Legacy removal (Django integration, InfluxDB code and settings, `mdb-postgres` env files): R3, its own change.
- Threaded gunicorn workers: R4.
- Map insert and station photos: R6, R7 (phase 3).
- Any change in monitoring_db or dtu_sus_web_server.
