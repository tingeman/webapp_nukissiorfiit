# Status: API v2 refactoring

Plan: [PLAN_api-v2.md](PLAN_api-v2.md). Branch: `agent-api-v2`.

## Now

Phase 1 approved 2026-11-15, code done (T1-T4). Next: **T5 (you)**, the check against the local backend, then T6 (release). Phase 2 approved 2026-11-15 (plan, "Phase 2 design" and T8-T15); next T8 (implementer) on `agent-api-v2-phase2` (from `agent-api-v2`; merge `agent-api-v2` into it when phase 1 changes).

## Done

- T1 2026-11-15: stub API v2 (v1/v2 metadata, `/v2/measurements/raw`, gzip), fixture generator `tests/fixtures/make_fixtures.py` (Tekbox + Campbell per mast, Mast 9 on two stations, M13 Tekbox without data, QAA2017-01 and SIS2024-07 to be filtered), 19 stub tests; suite 53 passed (`560adfd`)
- T2 2026-11-15: db_connector v2 next to the v1 functions: root URL, R1, metadata caches, `mast_catalogue`, chunked `/raw` (chunks counted from the start date, not 1 August), selections, pressure in kPa; suite 105 passed (`7f9b7d0`)
- T3 2026-11-15: page on v2: mast and logger dropdowns from the metadata, range follows the logger (date picker persistence removed: the logger callback sets the range at every load), one `/raw` request set per update, inclination by datatype, pressure in kPa, battery legend "Battery level" (was "B"); v1 data access, stub route and fixture removed; `influx_test`/`influxdb_client` imports dropped from app.py (unused); suite 103 passed (`48ed122`)
- T7 2026-11-15 (branch `agent-api-v2-phase2`): skeleton page at `/app/nuki/v2/`, a second Dash app on the same Flask server (`app/v2/`), links both ways, the classic data callback moved from the global `@callback` to `@app.callback` (Dash puts global callbacks into whichever app sets up first); suite 109 passed (`fd42b1d`)
- T4 2026-11-15: env template, README and cross-repo TODOs (`00a2f98`)

## Open questions

- monitoring_db: a site/group field for masts, and the inclinometer mounting orientation (plan, "Notes on the sensor definitions").

## Notes for the next session

- Phase 2 runs side by side with phase 1: a second Dash app at `/app/nuki/v2/` with links both ways, switch-over T15 (decided 2026-11-15). Phase 1 is released on its own when ready (T6). Phase 2 development (T7+) may start before T6 is deployed. Phase 2 is a free redesign; only the phase 1 page must stay available and unchanged.
- Sequencing: T2 adds the v2 functions alongside the v1 ones (app.py still uses v1); T3 switches the page and deletes the v1 functions, the v1 stub route and `tests/fixtures/measurements_v1.json`.
- From T1: with `station` the API prefixes column keys with the device serial (also with `serial`), so the connector uses `columns[]` details, never the key text. A station name match is ambiguous for Mast 9: always send identifiers. Metadata has no device type: logger labels use `display_name`.
- Decisions 2026-11-15: logger dropdown (default: the latest logger), always `/raw` in phase 1 (chunked, at most 2 in parallel), inclination selected by datatype `Angle`, keep `BACKEND_API_BASE_URL` and strip `/api/v1`.
- Stations shown: only identifiers fully matching `NUKI_STATION_PATTERN`, default `^(SIS2024-0[1-5]|NUKI202[0-9]-.+)$` (decision 2026-11-15).
- Bugs that phase 1 fixes on the way: Mast 13 showed the DTU test device (`feedbeefcafe0005`); pressure labelled kPa but stored in Pa. Pressure stays plotted in kPa: the connector converts it by the column's unit (decision 2026-11-15).
- Phase 2 decisions 2026-11-15: own client `app/v2/api_client.py` (TSP's Client, ApiError, daily/raw, rawseries, plus db_connector's catalogue, R1 connect-only retries, column selection, kPa); Daily default, Full at most 366 days (zoom refetches); latest logger default, All loggers on request; profile with both envelopes, the user chooses; table replaced by an Excel download (openpyxl) with own selectors and Full/Daily sheets per logger; Bootstrap bundled as in TSP; the per-station metadata requests at catalogue build accepted (also for v2's own cache).
- Follow-up 2026-11-15: the daily sheet has counts (the API's `/daily` already returns `count`); no zip (xlsx is already compressed); downloads get a time budget `DOWNLOAD_TIME_BUDGET_S` (default 75 s: budget + 30 s for a running request + workbook build < 120 s). nginx `proxy_read_timeout 120s` for `/app/nuki/` committed in dtu_sus_web_server `agent-fixes` (`396cc0a`, nginx -t passed); merging and deploying it is yours, before T14.
