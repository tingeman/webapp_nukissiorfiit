# User steps: API v2 refactoring

Plan: [PLAN_api-v2.md](PLAN_api-v2.md). Status: [STATUS_api-v2.md](STATUS_api-v2.md).

Steps only you can do, in order. Host-side steps (deploys, nginx, `main` of dtu_sus_web_server) are in dtu_sus_web_server `docs/plan/USER_STEPS_api-v2.md`.

All commands are for **WSL** (bash; Docker Desktop's `docker` works there). Every block starts with its `cd`, so it can be pasted from any directory.

**State 2026-11-15:** phase 1 released and deployed (`webapp_nuki 2026.2`). Phase 2 code is done (T8-T13 on `agent-api-v2-phase2`); no agent is working, so nothing here has to wait for one. Open: steps 2.4, 2.5, 3 and 4.

## 1. T5: check phase 1 against the local backend

- [x] Done 2026-11-15 (results in STATUS)

Done in a separate worktree of the phase 1 branch, `webapp_nuki_phase1` (compose mounts `./app` with `--reload`, so a checkout the agent edits would reload your running app):

```bash
# WSL
cd /mnt/c/thin/02_Code/python_web_apps
git -C webapp_nukissiorfiit worktree add ../webapp_nuki_phase1 agent-api-v2
cp -r webapp_nukissiorfiit/secrets webapp_nuki_phase1/      # dev env file + nginx certs (not in git)
cd webapp_nuki_phase1
docker compose --project-name nuki -f compose.develop.yml -f compose.develop.local.yml up -d --build webapp_nuki
```

Removing the worktree: step 2.6.

## 2. T6: release phase 1 (`2026.2`)

### 2.1 Git: fast-forward `develop/main`, tag, push

- [x] Done 2026-11-15

```bash
# WSL, webapp_nukissiorfiit
cd /mnt/c/thin/02_Code/python_web_apps/webapp_nukissiorfiit
git fetch . agent-api-v2:develop/main              # fast-forward local develop/main (refuses if not a fast-forward)
git tag -a 2026.2 develop/main -m "Phase 1: API v2 data access, logger selector"
git push origin agent-api-v2 develop/main 2026.2
```

### 2.2 Build and test

- [x] Done 2026-11-15 (tarball `e8c35fb0...`, 103 tests passed)

```bash
# WSL, dtu_sus_web_server (docs/deploy/releases.md; the script is the same file as dtu_sus_data02's)
cd /mnt/c/thin/02_Code/docker_projects/dtu_sus_web_server
scripts/build_image.sh webapp_nuki https://github.com/tingeman/webapp_nukissiorfiit.git 2026.2
```

### 2.3 Pin and deploy

- [x] Done 2026-11-15 (dtu_sus_web_server `da715d5`)

Bundle on the releases share, `ansible/vars/releases.yml` from the manifest in `release-out/webapp_nuki/`, then:

```bash
# WSL, dtu_sus_web_server, on develop/main with no uncommitted changes
cd /mnt/c/thin/02_Code/docker_projects/dtu_sus_web_server/ansible
ansible-playbook -kK -i hosts playbook_install_webapp_nuki.yml -v --ask-vault-pass
```

### 2.4 Check production

- [x] Done

The host commands and browser checks are in dtu_sus_web_server `USER_STEPS_api-v2.md`, step 3.

### 2.5 Fast-forward webapp_nukissiorfiit's `main` to the tag

- [x] Done

The tag `2026.2` lives in this app repository (webapp_nukissiorfiit), not in dtu_sus_web_server; that repository's `main` is step 4 of its own USER_STEPS. The first command checks that it is a fast-forward.

```bash
# WSL, webapp_nukissiorfiit
cd /mnt/c/thin/02_Code/python_web_apps/webapp_nukissiorfiit
git merge-base --is-ancestor main 2026.2 && git branch -f main '2026.2^{commit}' && git push origin main develop/main
```

`develop/main` is two commits ahead of the tag (the Dockerfile fixes `5359623` and `50037ad`, dev and build only, not needed in 2026.2); the push takes them along.

No env change is needed for phase 1: the `/api/v1` form of `BACKEND_API_BASE_URL` keeps working (one WARNING at start), and the new variables have defaults. **Keep the `/api/v1` form**: the nuki playbook's post-check requests `<BACKEND_API_BASE_URL>/metadata`, which fails with the root form (dtu_sus_web_server `USER_STEPS_api-v2.md`, Notes).

### 2.6 Remove the phase 1 worktree

- [x] Done

Stops your phase 1 dev app and removes the worktree (the branch `agent-api-v2` stays).

```bash
# WSL
cd /mnt/c/thin/02_Code/python_web_apps/webapp_nuki_phase1
docker compose --project-name nuki -f compose.develop.yml -f compose.develop.local.yml down
cd /mnt/c/thin/02_Code/python_web_apps
git -C webapp_nukissiorfiit worktree remove --force ../webapp_nuki_phase1   # --force: secrets/ is untracked
```

## 3. nginx timeout 120 s (needed before step 5)

- [ ] Done

`proxy_read_timeout 120s` for `/app/nuki/` is merged in dtu_sus_web_server (`a603fbe`) but not live: the nuki playbook does not reload nginx. Deploy it with `playbook_install_nginx.yml`: dtu_sus_web_server `USER_STEPS_api-v2.md`, steps 6-7.

## 4. Review phase 2

- [ ] Done (first look 2026-11-15: findings fixed in T16 `90b95a5`; second look open)

Review the commits and the open questions in STATUS:

```bash
# WSL, webapp_nukissiorfiit
cd /mnt/c/thin/02_Code/python_web_apps/webapp_nukissiorfiit
git log --oneline develop/main..agent-api-v2-phase2
```

Run the page against your local backend from this checkout (it is on `agent-api-v2-phase2`). Do step 2.6 first: both dev apps use project `nuki` and port 8050.

```bash
# WSL, webapp_nukissiorfiit
cd /mnt/c/thin/02_Code/python_web_apps/webapp_nukissiorfiit
docker compose --project-name nuki -f compose.develop.yml -f compose.develop.local.yml up -d --build webapp_nuki
```

Then `http://localhost:8050/app/nuki/v2/`. The agent checked only the default load and the profile by eye. Check: Daily/Full, a zoom in Full, the profile slider and the three envelopes, All loggers on Mast 6 and Mast 9, an Excel download of Mast 6 (All, all years; open it in Excel), the classic page `/app/nuki/` still unchanged. Note load times and anything that looks wrong; the agent fixes it on the phase 2 branch.

If the page hangs (also the classic one): rebuild, the dev image needs the fix `50037ad` (gunicorn `--threads 4`); `up -d --build` above does that.

## 5. T14: release phase 2 (after steps 3 and 4)

- [ ] Done

As step 2, with tag `2026.3`, after `agent-api-v2-phase2` is fast-forwarded into `develop/main` (your permission; it already contains `develop/main`). Host side: dtu_sus_web_server `USER_STEPS_api-v2.md`, step 8. Then on the server: `/app/nuki/v2/` Daily load times, a Full zoom, an All-loggers download for Mast 6, and the API log request counts (plan, "Effect on the API, per action").

## 6. T15: switch-over (your decision, later)

- [ ] Done

Tell the agent when phase 2 should replace phase 1 at `/app/nuki/`.

## Decisions for your review

The agent wrote open questions into STATUS (**Open questions**) instead of stopping, with the default it picked. Answer them in step 4.
