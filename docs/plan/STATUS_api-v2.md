# Status: API v2 refactoring

Plan: [PLAN_api-v2.md](PLAN_api-v2.md). Branch: `agent-api-v2`.

## Now

Plan drafted 2026-11-15, waiting for approval. Next: create `agent-api-v2` from `develop/api_v2/main`, then T1 (implementer).

## Done

## Open questions

- Phase 2: default mode of the Daily/Full switch, and the threshold (to decide in the phase 2 planning session).
- monitoring_db: a site/group field for masts, and the inclinometer mounting orientation (plan, "Notes on the sensor definitions").

## Notes for the next session

- Phase 2 runs side by side with phase 1: a second Dash app at `/app/nuki/v2/` with links both ways, switch-over T14 (decided 2026-11-15). Phase 1 is released on its own when ready (T6). Phase 2 development (T7+) may start before T6 is deployed.

- Decisions 2026-11-15: logger dropdown (default: the latest logger), always `/raw` in phase 1 (chunked, at most 2 in parallel), inclination selected by datatype `Angle`, keep `BACKEND_API_BASE_URL` and strip `/api/v1`.
- Stations shown: only identifiers fully matching `NUKI_STATION_PATTERN`, default `^(SIS2024-0[1-5]|NUKI202[0-9]-.+)$` (decision 2026-11-15).
- Bugs that phase 1 fixes on the way: Mast 13 showed the DTU test device (`feedbeefcafe0005`); pressure labelled kPa but stored in Pa. Pressure stays plotted in kPa: the connector converts it by the column's unit (decision 2026-11-15).
