# Roadmap: webapp_nuki

Planned work for the app. Each item says where it came from.

## Ongoing

| Plan | Items | Status |
|---|---|---|
| [PLAN_api-v2.md](plan/PLAN_api-v2.md) ([status](plan/STATUS_api-v2.md)) | Phase 1: R1, R2. Phase 2: R5 | draft 2026-11-15 |

## Phase 1: API v2 (in PLAN_api-v2)

| # | Item | Why | Source |
|---|---|---|---|
| R1 | **Do not retry HTTP 504.** `_build_session` (`app/db_connector.py:31-45`) retries `status_forcelist=[504]` up to `BACKEND_API_MAX_RETRIES` times (default 2). A 504 from the API is its 20 s statement timeout (`QUERY_TIMEOUT`), so the same expensive query runs up to three times: about 60 s of database work for one click, and longer than nginx's 60 s proxy timeout on sus-parcweb01. Keep retries for connection errors only; show the timeout message at once. | Load on the database host, user-visible 503s | `dtu_sus_web_server/docs/plan/PLAN_capacity-tuning.md`, F5 and T7 (2026-11-14) |
| R2 | **Move from API v1 to v2 and bound each request.** Today each dashboard update makes up to 5 **sequential** v1 requests (`/metadata`, `/measurements`, `app/db_connector.py:147-204`), each for the whole selected range in one request. v1 builds JSON rows in Python on the API side and its responses are about 5x larger than v2's columnar ones (`dtu_sus_monitoring_db/docs/read_api.md`). For v2: use `/api/v2/measurements/daily` for long ranges and `/raw` for short ones (as the GRL TSP app does), split long ranges into chunks, run independent requests in parallel with a small cap (the TSP app uses `MDB_API_MAX_PARALLEL`, planned value 2), and send `Accept-Encoding: gzip`. Consider threaded gunicorn workers (`--worker-class gthread --threads 4`, as webapp_grl_tsp) once requests are I/O-bound waits (now R4). Phase 1 uses `/raw` only (decision 2026-11-15); `/daily` comes with R5. | Response time for users, load on the monitoring_db API (4 workers shared by all apps) | `dtu_sus_web_server/docs/plan/PLAN_capacity-tuning.md`, F5 (2026-11-14) |

Reference implementation for R2: `webapp_grl_tsp` (`app/api_client.py`: chunked, parallel, error
mapping; `app/rawseries.py`: raw data thinned for plotting).

## Phase 2: page aligned with webapp_grl_tsp (in PLAN_api-v2, tasks T7-T15)

| # | Item | Why | Source |
|---|---|---|---|
| R5 | **TSP-style page for the Nukissiorfiit stations.** Module split (layout/callbacks/figures/texts); Daily/Full switch (`/daily` overview, `/raw` thinned and refetched for the zoom window); all sensor groups in one figure with **linked x axes** (zoom and pan apply to all); inclination angles in separate rows with their own scales; heading and sensor temperature panels; ground temperature **profile** for one day (daily mean against depth, min/max band) with a **day slider**; "All loggers" view of a mast; loading indicator; date limits from metadata; fix or drop the table. | Same user experience as the TSP app; less API load in the default view | You (2026-11-15); README TODO list (depth graph with slider, linked zoom, inclination in 3 linked axes, table without timestamps, "working on it" indicator, end date / session persistence) |

## Phase 3 and later

| # | Item | Why | Source |
|---|---|---|---|
| R3 | **Remove legacy code** as its own change: `app/django_integration/`, `app/influx_test.py`, `app/mdb_test.py`, `app/validate_release_stage_c.py`, `submit_to_influxdb.py`, the `influxdb_client` import in `app.py`, the required `INFLUXDB_*` settings in `config.py` (and then in the template and, at a later deploy, in the production env file in dtu_sus_web_server), and the `mdb-postgres` env files. | Dead code, an unused secret in production | README TODO, `CROSS_REPO_DEV_README.md` TODOs, CLAUDE.md |
| R4 | **Threaded gunicorn workers.** `--worker-class gthread --threads N` once callbacks only wait for the API. Size it against the API: with `BACKEND_API_MAX_PARALLEL=2`, 4 workers x 4 threads could send 32 requests at once to the API's 4 workers. Possibly fewer workers with threads, or a per-process request semaphore. Needs `thread_sensitive=False` (phase 1 T2). | Responsiveness under concurrent users without overloading the API | R2; PLAN_api-v2 Assumptions |
| R6 | **Map insert** with the station location. Needs `lat/lon` in the `/metadata` stations (monitoring_db change; the station definitions have them). | README TODO | README |
| R7 | **Station photo.** Where photos are stored and served is to be decided (static assets in the image, or served by monitoring_db or nginx). | README TODO | README |
| R8 | **Suggestions for monitoring_db** (to raise there, not change here): a site/group field on Station, so that Mast 9's two stations (SIS2024-03, NUKI2026-M9) are grouped without relying on `name`; record the inclinometer mounting orientation (Tekbox Roll/Pitch vs Campbell X/Y); consistent `relative_z` for BatteryVoltage. | Robust mast grouping; comparable inclination across the logger change | PLAN_api-v2, "Notes on the sensor definitions" |
| R9 | **README cleanup.** "Installation" and "Direct Python Run" describe a local pip install of `requirements.txt` without hashes, which the project no longer uses (tests and runs are in Docker). The Notes section still speaks of the Django ORM. | Outdated instructions | README |

## Finished plans

| Plan | Items | Done |
|---|---|---|
