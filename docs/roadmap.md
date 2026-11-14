# Roadmap: webapp_nuki

Planned work for the app. Items here are deferred to the **major refactoring of the app onto the
monitoring_db API v2** (not yet planned in detail). Each item says where it came from.

## Deferred to the API v2 refactoring

| # | Item | Why | Source |
|---|---|---|---|
| R1 | **Do not retry HTTP 504.** `_build_session` (`app/db_connector.py:31-45`) retries `status_forcelist=[504]` up to `BACKEND_API_MAX_RETRIES` times (default 2). A 504 from the API is its 20 s statement timeout (`QUERY_TIMEOUT`), so the same expensive query runs up to three times: about 60 s of database work for one click, and longer than nginx's 60 s proxy timeout on sus-parcweb01. Keep retries for connection errors only; show the timeout message at once. | Load on the database host, user-visible 503s | `dtu_sus_web_server/docs/plan/PLAN_capacity-tuning.md`, F5 and T7 (2026-11-14) |
| R2 | **Move from API v1 to v2 and bound each request.** Today each dashboard update makes up to 5 **sequential** v1 requests (`/metadata`, `/measurements`, `app/db_connector.py:147-204`), each for the whole selected range in one request. v1 builds JSON rows in Python on the API side and its responses are about 5x larger than v2's columnar ones (`dtu_sus_monitoring_db/docs/read_api.md`). For v2: use `/api/v2/measurements/daily` for long ranges and `/raw` for short ones (as the GRL TSP app does), split long ranges into chunks, run independent requests in parallel with a small cap (the TSP app uses `MDB_API_MAX_PARALLEL`, planned value 2), and send `Accept-Encoding: gzip`. Consider threaded gunicorn workers (`--worker-class gthread --threads 4`, as webapp_grl_tsp) once requests are I/O-bound waits. | Response time for users, load on the monitoring_db API (4 workers shared by all apps) | `dtu_sus_web_server/docs/plan/PLAN_capacity-tuning.md`, F5 (2026-11-14) |

Reference implementation for R2: `webapp_grl_tsp` (`app/api_client.py`: chunked, parallel, error
mapping; `app/rawseries.py`: raw data thinned for plotting).
