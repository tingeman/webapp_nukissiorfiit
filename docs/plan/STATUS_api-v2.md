# Status: API v2 refactoring

Plan: [PLAN_api-v2.md](PLAN_api-v2.md). Your steps: [USER_STEPS_api-v2.md](USER_STEPS_api-v2.md). Branch: `agent-api-v2` (phase 1), `agent-api-v2-phase2` (phase 2).

## Now

Phase 1 approved 2026-11-15, code done (T1-T4). T5 checked by you 2026-11-15. Next: **T6 (you)**, the release. Phase 2 approved 2026-11-15 (plan, "Phase 2 design" and T8-T15); T8-T10 done; next T11 (implementer) on `agent-api-v2-phase2` (from `agent-api-v2`; merge `agent-api-v2` into it when phase 1 changes).

## Done

- T1 2026-11-15: stub API v2 (v1/v2 metadata, `/v2/measurements/raw`, gzip), fixture generator `tests/fixtures/make_fixtures.py` (Tekbox + Campbell per mast, Mast 9 on two stations, M13 Tekbox without data, QAA2017-01 and SIS2024-07 to be filtered), 19 stub tests; suite 53 passed (`560adfd`)
- T2 2026-11-15: db_connector v2 next to the v1 functions: root URL, R1, metadata caches, `mast_catalogue`, chunked `/raw` (chunks counted from the start date, not 1 August), selections, pressure in kPa; suite 105 passed (`7f9b7d0`)
- T3 2026-11-15: page on v2: mast and logger dropdowns from the metadata, range follows the logger (date picker persistence removed: the logger callback sets the range at every load), one `/raw` request set per update, inclination by datatype, pressure in kPa, battery legend "Battery level" (was "B"); v1 data access, stub route and fixture removed; `influx_test`/`influxdb_client` imports dropped from app.py (unused); suite 103 passed (`48ed122`)
- T7 2026-11-15 (branch `agent-api-v2-phase2`): skeleton page at `/app/nuki/v2/`, a second Dash app on the same Flask server (`app/v2/`), links both ways, the classic data callback moved from the global `@callback` to `@app.callback` (Dash puts global callbacks into whichever app sets up first); suite 109 passed (`fd42b1d`)
- T8 2026-11-15: `app/v2/api_client.py` (Client, ApiError, daily/raw sharing `_fetch` with max_parallel and a deadline, daily chunks at 1 August, catalogue and caches, selections incl. heading/sensor temp, kPa), rawseries/trumpet ported, stub `/daily`, env template lists the two new variables; suite 169 passed (`61298b4`)
- T9 2026-11-15: v2 page: selectors from the catalogue, period from the logger, Daily with min/max bands, Full (one year up to the end date, one raw request, thinned, refetch on zoom by Patch), one `make_subplots` figure, info line, Bootstrap bundled (sha256 as TSP, `-text` in `.gitattributes`); suite 196 passed (`2a12010`)
- T5 2026-11-15 (you, local backend): 5 masts with the expected loggers; default ranges have data; inclination Roll/Pitch (Tekbox) and X/Y (Campbell); pressure about 100 kPa for both; 1-3 contiguous `/raw` chunks per update (a picker edit of start and end is 2 updates); backend paused: "Backend request timed out" after the read timeout, no retry and no second GET; nginx stopped: 2 connect retries, then "An unknown error occurred"; API time for one Tekbox year (Mast 6) 0.5-1.4 s, 27-33 KB gzip (plan estimate was about 1 MB: the real data is much sparser)
- Phase 1 fix 2026-11-15 (on `agent-api-v2`, your request): an unreachable backend shows "Backend not reachable" (`BackendUnreachable`, a RuntimeError), logged without a traceback; phase 1 suite 103 passed (`655aa0a`), merged into the phase 2 branch (`3d547be`)
- T10 2026-11-15: profile panel (`app/v2/profile.py`): day slider over every loaded day, Day min/max band, annual envelope via `trumpet.trumpet` on a 365-day window (TSP's 90 % rule), or both; compact `profile` store; day marker patched onto the figure; Full makes 1 raw then 1 daily request, in sequence; suite 217 passed (`af2206c`)
- T4 2026-11-15: env template, README and cross-repo TODOs (`00a2f98`)

## Open questions


- Full limit: implemented as "one year up to the end date" (365 or 366 days) instead of "366 days", so Full is always one raw request (a 366-day window over a non-leap year was 2 requests). Agent decision 2026-11-15, within the plan's intent; say if you want 366 days back.
- Profile: the store (about 80 KB for 5 depths, 155 KB for 10, over 2 years) goes back to the server on every slider release, since the profile is drawn server-side (tested with trumpet.py). A clientside JS callback would avoid the upload and could follow the slider while dragging, but duplicates the envelope logic in JS without Python tests. Kept server-side; revisit if the slider feels slow on the server.
- The v2 page has not been looked at by eye yet (tests only); see USER_STEPS step 4.

- monitoring_db: a site/group field for masts, and the inclinometer mounting orientation (plan, "Notes on the sensor definitions").

## Notes for the next session

- T8 choices: `API_TIMEOUT` has no seconds field (also used for 504); daily series keep only days with count > 0; `Selected.unit` holds the converted unit (details keep the API's); an empty selection returns `[]`; an invalid logger value raises `ValueError`; metadata caches are per `Client` instance, so T9 creates one client per process at import.
- T9 choices: default mast = first in catalogue order (Mast 6); a range change reloads at once (no Load button, `updatemode="bothdates"`); the classic page has no assets folder, so it loads no v2 CSS (tested); `test_pages.test_callbacks_apart` now checks callback modules, since both pages have a `logger.options/value` output.

- Phase 2 runs side by side with phase 1: a second Dash app at `/app/nuki/v2/` with links both ways, switch-over T15 (decided 2026-11-15). Phase 1 is released on its own when ready (T6). Phase 2 development (T7+) may start before T6 is deployed. Phase 2 is a free redesign; only the phase 1 page must stay available and unchanged.
- Sequencing: T2 adds the v2 functions alongside the v1 ones (app.py still uses v1); T3 switches the page and deletes the v1 functions, the v1 stub route and `tests/fixtures/measurements_v1.json`.
- From T1: with `station` the API prefixes column keys with the device serial (also with `serial`), so the connector uses `columns[]` details, never the key text. A station name match is ambiguous for Mast 9: always send identifiers. Metadata has no device type: logger labels use `display_name`.
- Decisions 2026-11-15: logger dropdown (default: the latest logger), always `/raw` in phase 1 (chunked, at most 2 in parallel), inclination selected by datatype `Angle`, keep `BACKEND_API_BASE_URL` and strip `/api/v1`.
- Stations shown: only identifiers fully matching `NUKI_STATION_PATTERN`, default `^(SIS2024-0[1-5]|NUKI202[0-9]-.+)$` (decision 2026-11-15).
- Bugs that phase 1 fixes on the way: Mast 13 showed the DTU test device (`feedbeefcafe0005`); pressure labelled kPa but stored in Pa. Pressure stays plotted in kPa: the connector converts it by the column's unit (decision 2026-11-15).
- Phase 2 decisions 2026-11-15: own client `app/v2/api_client.py` (TSP's Client, ApiError, daily/raw, rawseries, plus db_connector's catalogue, R1 connect-only retries, column selection, kPa); Daily default, Full at most 366 days (zoom refetches); latest logger default, All loggers on request; profile with both envelopes, the user chooses; table replaced by an Excel download (openpyxl) with own selectors and Full/Daily sheets per logger; Bootstrap bundled as in TSP; the per-station metadata requests at catalogue build accepted (also for v2's own cache).
- Follow-up 2026-11-15: the daily sheet has counts (the API's `/daily` already returns `count`); no zip (xlsx is already compressed); downloads get a time budget `DOWNLOAD_TIME_BUDGET_S` (default 75 s: budget + 30 s for a running request + workbook build < 120 s). nginx `proxy_read_timeout 120s` for `/app/nuki/` committed in dtu_sus_web_server `agent-fixes` (`396cc0a`, nginx -t passed); merging and deploying it is yours, before T14.
- Branches: work on `agent-api-v2-phase2` from now (T8 onward). The plan/status commit `34c90b3` exists only there; `agent-api-v2` stops at `79f091b`. If phase 1 changes, merge `agent-api-v2` into the phase 2 branch, and in a STATUS conflict keep the phase 2 version plus the phase 1 lines.
