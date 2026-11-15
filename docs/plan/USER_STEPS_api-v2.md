# User steps: API v2 refactoring

Plan: [PLAN_api-v2.md](PLAN_api-v2.md). Status: [STATUS_api-v2.md](STATUS_api-v2.md).

Steps only you can do, in order. Each says whether you can do it **while the agent works** on phase 2 (T8-T13, branch `agent-api-v2-phase2`, in this checkout) or must **wait**.

The agent works in this checkout (`C:\thin\02_Code\python_web_apps\webapp_nukissiorfiit`), switches nothing but `agent-api-v2-phase2`, builds `webapp_nuki:local` and runs `tests/run.sh` (containers with `--network none`, no ports). It does not use compose, port 8050 or the project name `nuki`.

## 1. T5: check phase 1 against the local backend (can run in parallel)

Don't run compose in this checkout while the agent works: `compose.develop.yml` mounts `./app` with `--reload`, so the agent's edits would land in your running app. Use a separate worktree of the phase 1 branch:

```bash
cd /c/thin/02_Code/python_web_apps
git -C webapp_nukissiorfiit worktree add ../webapp_nuki_phase1 agent-api-v2
cp -r webapp_nukissiorfiit/secrets webapp_nuki_phase1/      # dev env file + nginx certs (not in git)
cd webapp_nuki_phase1
docker compose --project-name nuki -f compose.develop.yml -f compose.develop.local.yml up -d --build webapp_nuki
```

Backend: dev database restored and monitoring_db up (`CROSS_REPO_DEV_README.md`). Then open `http://localhost:8050/app/nuki/` and check:

- 5 masts; Mast 9 with 2 loggers (SIS2024-03 Tekbox, NUKI2026-M9 Campbell); Mast 13 only the CR350 12216 (no test device `feedbeefcafe0005`).
- For each logger the default range (last 30 days of its data) shows data.
- Inclination: Roll/Pitch (Tekbox), X/Y (Campbell); no heading or sensor temperature.
- Pressure about 100 kPa.
- Backend log: 1-3 `/api/v2/measurements/raw` per update, no repeats after a timeout.
- Note the response time for a full logger period (set the range to the logger's whole life).

Tell the agent (or write into STATUS under Done/T5) what you found. Bugs found here are fixed on `agent-api-v2` and merged into the phase 2 branch.

Clean up afterwards:

```bash
docker compose --project-name nuki -f compose.develop.yml -f compose.develop.local.yml down
cd .. && git -C webapp_nukissiorfiit worktree remove --force ../webapp_nuki_phase1   # --force: secrets/ is untracked
```

## 2. T6: release phase 1 (can run in parallel, after step 1)

Independent of the phase 2 work (other branch; the release builds from GitHub, not from this checkout).

1. Push and fast-forward: `git push origin agent-api-v2`, then fast-forward `develop/main` to `agent-api-v2` (or tell the agent "fast-forward develop/main to agent-api-v2"; it then does only that merge, the push stays yours).
2. Tag on `develop/main` and push the tag.
3. In dtu_sus_data02: `scripts/build_image.sh webapp_nuki https://github.com/tingeman/webapp_nukissiorfiit.git <tag>`.
4. In dtu_sus_web_server: set the version in `ansible/vars/releases.yml`, deploy with its playbook.
5. Optional, at the next edit of the production env file: `BACKEND_API_BASE_URL` in the root form (`http://<data02-host>/mdb`); the `/api/v1` form keeps working.

## 3. nginx timeout 120 s (can run in parallel; needed before step 5)

dtu_sus_web_server branch `agent-fixes` (`396cc0a`): `proxy_read_timeout 120s` for `/app/nuki/`.

1. Merge `agent-fixes` into its `develop/main`.
2. Deploy nginx (`playbook_install_nginx.yml`).
3. `curl -sI https://arctic.sustain.dtu.dk/app/nuki/` still answers 200.

## 4. Review phase 2 (wait for the agent)

When the agent stops, it reports which of T8-T13 are done (STATUS, **Now**). Review the commits on `agent-api-v2-phase2`. Look at the page locally the same way as step 1, but from this checkout once the agent has finished (or from a worktree of `agent-api-v2-phase2`): `http://localhost:8050/app/nuki/v2/`. Check Daily/Full, a zoom in Full, the profile slider, All loggers on Mast 9 and Mast 6, and an Excel download of Mast 6 (All, all years). Note load times.

## 5. T14: release phase 2 (wait for the agent and your review; after step 3)

As step 2, with `agent-api-v2-phase2` merged into `develop/main` (your permission). Then on the server: `/app/nuki/v2/` Daily load times, a Full zoom, an All-loggers download for Mast 6, and the API log request counts (plan, "Effect on the API, per action").

## 6. T15: switch-over (your decision, later)

Tell the agent when phase 2 should replace phase 1 at `/app/nuki/`.

## Decisions the agent may ask for

The agent writes open questions into STATUS (**Open questions**) instead of stopping, picks the plan's default, and notes it. Answer them when you review (step 4).
