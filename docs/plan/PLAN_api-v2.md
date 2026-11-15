# Plan: API v2 refactoring (phase 1: v2 data access and logger selector; phase 2: TSP-style page)

Status: **draft (2026-11-15)**. Roadmap: R1, R2 (phase 1); R5 (phase 2). Branch: `agent-api-v2` (from `develop/api_v2/main`).

## Goal

Phase 1 (as few changes to the page as possible):

1. The app reads only through the monitoring_db API v2 (`/api/v2/measurements/raw`, `/api/v2/metadata`), plus `/api/v1/metadata` for the station list (see Design notes).
2. R1: no retries of HTTP 504; a timeout shows its message at once.
3. R2: one request set per dashboard update instead of 5 sequential v1 requests, with each request bounded (1-year chunks, at most 2 in parallel, gzip).
4. Masts and their loggers come from the API metadata, not from hard-coded DevEUIs. A logger dropdown selects the Tekbox (2024-09 to 2026-09) or the Campbell CR350 (from 2026-09) of a mast.
5. Inclination works for both sensor types (Tekbox TBSCS1: RollAngle/PitchAngle; Campbell SOLAR-2-45: XAngle/YAngle).

Phase 2 (align with webapp_grl_tsp, Nukissiorfiit stations only):

6. TSP code structure (layout, callbacks, figures, texts, api client), Daily/Full toggle, all sensor panels in one figure with linked x axes, a ground temperature profile for a chosen day (daily mean, min/max band) with a day slider.

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
| Metadata | 2 x whole-database `/v1/metadata`, every update | `/v1/metadata` once per 10 min per worker; `/v2/metadata?station=` 1-2 per mast per 10 min per worker |
| Measurements | 3 sequential `/v1/measurements`, whole range each (JSON rows built in Python) | 1 to 3 `/v2/.../raw` chunks of at most 1 year, at most 2 in parallel, columnar, gzip |
| Worst case in parallel per app worker | 1 | 2 |

Always raw (user decision 2026-11-15): a 2-year range of one logger is about 17 000 timestamps x 20 columns; gzip makes that about 1 MB on the wire. The plots keep `lines+markers` with all points, as today. Thinning and `/daily` come in phase 2 (T8).

### Page (T3), minimal change

- Mast dropdown: options from the catalogue (the hard-coded list goes). Logger dropdown (new) under it: options for the selected mast, default the logger with the latest `last_measurement_time`. Loggers without measurements (`last_measurement_time` null) are left out: Mast 13 shows only its CR350 (12216, live since 2026-09-26) until its Tekbox (0008) data is ingested, and then shows both without a code change.
- When the logger changes, the date range is set to the last 30 days of that logger's data (today: the last month before today, which would be empty for a Tekbox). The picker's `min/max_date_allowed` come from the logger's first/last measurement. The user can still change the range afterwards.
- One callback fetches the frame and builds the same 6 graphs from it, with one error handler (the messages stay those of today: timeout, no data, logger not found, unknown error).
- Titles name the mast and logger (`Ground Temperature - Mast 6 (CR350 12096)`).
- The table keeps showing the ground temperatures (the missing timestamps column is fixed in phase 2, T12).

### Phase 2 outline (refined in its own planning session before T7)

**Side by side with phase 1 (user requirement 2026-11-15).** The phase 1 page stays until you decide to switch, and users can move between the two pages. Decided 2026-11-15 (path `/app/nuki/v2/` chosen by you):

- Phase 2 is a second `Dash` instance on the same Flask `server`, in its own package (`app/v2/`), at `/app/nuki/v2/`. Phase 1 stays at `/app/nuki/` unchanged. Each page has a link to the other ("Try the new page" / "Back to the classic page").
- No deployment change: nginx's `location /app/nuki/` already proxies the sub-path, and the port, healthcheck (`/app/nuki/`) and env are unchanged. Separate Dash instances keep component IDs and callbacks apart.
- Shared: `config.py` and the API data access (phase 1's `db_connector` catalogue, metadata cache and chunked raw fetch; phase 2 adds `/daily`). Phase 2 must not change phase 1's behaviour; phase 1's tests keep running.
- The switch-over is its own task (T14, after your decision): swap the routes (new page at `/app/nuki/`, classic at `/app/nuki/classic/` for a while or removed), then remove the classic page as a separate change.
- API effect: two pages share the same per-process caches. Load only grows as much as users actually use the new page.

Taken from the TSP app: module split (`layout.py`, `callbacks.py`, `figures.py`, `texts.py`, `api_client.py`), the Daily/Full switch (`/daily` for the overview; `/raw` thinned with `rawseries.thin` and refetched for the zoom window via `relayoutData`), `dcc.Loading`, the timing/size info line, and the error texts. Different from TSP: every sensor group is shown, all in **one `make_subplots` figure with `shared_xaxes`**, so zoom and pan apply to all rows (README TODO): ground temperature, air temperature, humidity, pressure, one row per inclination angle with its own scale (README TODO), heading, sensor temperature, battery. Daily mode draws the mean with a min/max band. The **ground temperature profile**: temperature against depth for one day (daily mean, min/max shaded), with a slider over the days of the loaded range. It uses `/daily` data that the overview already loaded (in Full mode one extra `/daily` request set for the period). A **combined "All loggers" view** of a mast joins the records by depth (the strings were redeployed with the same serials on Masts 6 and 12; Mast 9 has a new string, which stays separate).

## Assumptions

- Branch `agent-api-v2` from `develop/api_v2/main` (the branch you are on).
- Stations are filtered by identifier pattern (decided), and masts are grouped by station `name` (assumed). If monitoring_db later gets a site/group field, the catalogue switches to it (see the DB notes below).
- `calibrated` and `include_masked` are not sent (API defaults: calibrated values, masked values left out), as in TSP. No calibrations are registered for these stations yet, so the values do not change.
- The InfluxDB settings stay required in phase 1. Their removal goes with the legacy cleanup (R3) as a separate change.
- gunicorn stays at 4 sync workers. Threaded workers are deferred (R4): with 2 parallel requests per callback, 4 workers x 4 threads could put 32 requests on the API's 4 workers.
- No local Python: all tests run in Docker (`tests/run.sh`). Ruff, mypy and pytest are not set up.

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

### Phase 2 (tasks refined in a planning session; the list is the intended scope)

Phase 1 is released on its own when it is ready (T6). Phase 2 development may start before T6 is deployed, on the same branch or a branch from it. Phase 2 is released later (T13), when you decide.

- **T7.** New page skeleton in `app/v2/` (`layout.py`, `callbacks.py`, `figures.py`, `texts.py`, TSP structure), mounted at `/app/nuki/v2/` on the same server, with links between the two pages. Phase 1's `app.py` stays as it is (only the link is added). Tests: both pages answer 200, and the phase 1 tests are unchanged. Agent: implementer.
- **T8.** Daily/Full switch: `/api/v2/measurements/daily` in the connector (3-year chunks), daily figures with a min/max band; Full mode thinned (`rawseries.thin`, about 2000 points per series) and refetched for the zoom window (`relayoutData`). API effect: the default view drops to 1 small daily request set; Full adds 1 bounded raw set per zoom. Agent: implementer.
- **T9.** One figure with linked x axes for all panels (ground temperature, air temperature, humidity, pressure, one row per inclination angle with its own scale, heading, sensor temperature, battery). Agent: implementer.
- **T10.** Ground temperature profile with day slider (daily mean against depth, min/max shaded). Agent: implementer.
- **T11.** "All loggers" view of a mast (joined by depth, a marker at the logger change). API effect: one request set per logger (2 instead of 1). Agent: implementer.
- **T12.** Small items: `dcc.Loading` indicator, date limits from metadata (fixes the README TODO about the end date and session persistence), the table (timestamps column, or drop the table), the timing/size info line. Agent: implementer.
- **T13.** Release phase 2 alongside phase 1 (you), as T6.
- **T14.** Switch-over, only after your decision: the new page goes to `/app/nuki/`, the classic page moves to `/app/nuki/classic/` or is removed (its removal is a separate commit). Check the healthcheck path. Agent: implementer.

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
