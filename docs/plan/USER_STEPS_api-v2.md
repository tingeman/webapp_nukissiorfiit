# User steps: API v2 refactoring

Plan: [PLAN_api-v2.md](PLAN_api-v2.md). Status: [STATUS_api-v2.md](STATUS_api-v2.md).

Steps only you can do, in order. Each says whether you can do it **while the agent works** on phase 2 (T8-T13, branch `agent-api-v2-phase2`, in this checkout) or must **wait**.

The agent works in this checkout (`C:\thin\02_Code\python_web_apps\webapp_nukissiorfiit`), switches nothing but `agent-api-v2-phase2`, builds `webapp_nuki:local` and runs `tests/run.sh` (containers with `--network none`, no ports). It does not use compose, port 8050 or the project name `nuki`.

## 1. T5: check phase 1 against the local backend (DONE 2026-11-15, results in STATUS)

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

The "Backend not reachable" fix (`655aa0a`) is already on `agent-api-v2`, so it ships with this release; nothing extra to do. The release is built from GitHub, so everything must be pushed first.

**a. Git (Git Bash, in either checkout; both share one repository, no branch switch needed):**

```bash
cd /c/thin/02_Code/python_web_apps/webapp_nukissiorfiit
git fetch origin                                   # check origin/develop/main is still 1ef45c7 (tag 2026.1)
git log --oneline -1 origin/develop/main
git fetch . agent-api-v2:develop/main              # fast-forward local develop/main (refuses if not a fast-forward)
git tag -a 2026.2 develop/main -m "Phase 1: API v2 data access, logger selector"
git push origin agent-api-v2 develop/main 2026.2
```

(`2026.2` follows `2026.1`; pick another name if you prefer.)

**b. Build and test (WSL, in dtu_sus_web_server; its `docs/deploy/releases.md`, the script is the same file as dtu_sus_data02's):**

```bash
scripts/build_image.sh webapp_nuki https://github.com/tingeman/webapp_nukissiorfiit.git 2026.2
```

It clones the tag, runs `tests/run.sh` (103 tests) and writes the bundle to dtu_sus_web_server's `release-out/webapp_nuki/`.

**c. Deploy (dtu_sus_web_server, `docs/deploy/releases.md`):**

1. Publish the bundle files to the releases share (`...\SUS-PArcWeb01\releases\webapp_nuki\`).
2. In `ansible/vars/releases.yml`, `webapp_nuki`: `version` and `tarball_sha256` from the manifest in `release-out/webapp_nuki/`, and update the comment. Commit.
3. From `ansible/`: `ansible-playbook -kK -i hosts playbook_install_webapp_nuki.yml -v --ask-vault-pass`.
4. Check `https://arctic.sustain.dtu.dk/app/nuki/`: masts, loggers, a Campbell and a Tekbox logger.
5. Fast-forward `main` to the deployed tag: `git fetch . 2026.2:main && git push origin main` (in this repository).

No env change is needed: the `/api/v1` form of `BACKEND_API_BASE_URL` keeps working (one WARNING at start), and the new variables have defaults. Optional, at the next edit of the production env file: the root form `http://<data02-host>/mdb`.

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
