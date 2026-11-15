# Status: API v2 refactoring

Plan: [PLAN_api-v2.md](PLAN_api-v2.md). Branch: `agent-api-v2`.

## Now

Plan approved 2026-11-15. T2 (implementer) running. T4 edits are in the working tree, uncommitted: commit after T2 (fix the template's code-default line to the root form). Then T3; T7 (phase 2 skeleton, branch from `agent-api-v2`) may run alongside T3.

## Done

- T1 2026-11-15: stub API v2 (v1/v2 metadata, `/v2/measurements/raw`, gzip), fixture generator `tests/fixtures/make_fixtures.py` (Tekbox + Campbell per mast, Mast 9 on two stations, M13 Tekbox without data, QAA2017-01 and SIS2024-07 to be filtered), 19 stub tests; suite 53 passed (`560adfd`)

## Open questions

- Phase 2: default mode of the Daily/Full switch, and the threshold (to decide in the phase 2 planning session).
- monitoring_db: a site/group field for masts, and the inclinometer mounting orientation (plan, "Notes on the sensor definitions").

## Notes for the next session

- Phase 2 runs side by side with phase 1: a second Dash app at `/app/nuki/v2/` with links both ways, switch-over T14 (decided 2026-11-15). Phase 1 is released on its own when ready (T6). Phase 2 development (T7+) may start before T6 is deployed. Phase 2 is a free redesign; only the phase 1 page must stay available and unchanged.
- Sequencing: T2 adds the v2 functions alongside the v1 ones (app.py still uses v1); T3 switches the page and deletes the v1 functions, the v1 stub route and `tests/fixtures/measurements_v1.json`.
- From T1: with `station` the API prefixes column keys with the device serial (also with `serial`), so the connector uses `columns[]` details, never the key text. A station name match is ambiguous for Mast 9: always send identifiers. Metadata has no device type: logger labels use `display_name`.
- Decisions 2026-11-15: logger dropdown (default: the latest logger), always `/raw` in phase 1 (chunked, at most 2 in parallel), inclination selected by datatype `Angle`, keep `BACKEND_API_BASE_URL` and strip `/api/v1`.
- Stations shown: only identifiers fully matching `NUKI_STATION_PATTERN`, default `^(SIS2024-0[1-5]|NUKI202[0-9]-.+)$` (decision 2026-11-15).
- Bugs that phase 1 fixes on the way: Mast 13 showed the DTU test device (`feedbeefcafe0005`); pressure labelled kPa but stored in Pa. Pressure stays plotted in kPa: the connector converts it by the column's unit (decision 2026-11-15).
