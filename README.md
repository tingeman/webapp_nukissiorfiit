# webapp_nukissiorfiit

## TODO list
[] Date range does not seem to go beyond 2025-01-30?? This seems to be related to session persistence. Fix to update max_end_date dynamically attempted (is it fixed?) (planned: docs/plan/PLAN_api-v2.md, phase 2)
[x] Filtering of data should include all datapoints on the last date of th  e daterange 
[] New graph with temperature as a function of depth. A slider should allow user to iterate through the entire date range. Should plot the daily average at each sensor depth. (planned: docs/plan/PLAN_api-v2.md, phase 2)
[] Something is wrong with the table of measurements, it doesn't show timestamps (planned: docs/plan/PLAN_api-v2.md, phase 2)
[] Inclinations in 3 separate but linked axes, so that variations are visible (individual scales) (planned: docs/plan/PLAN_api-v2.md, phase 2)
[] Map insert with location of station (grab location from station table in database) (roadmap R6)
[] Photo of station (we will have to discuss how to best implement this) (roadmap R7)
[] Remove old code that interacts with influxdb (roadmap R3)
[] Link the zooming in all axes (zoom in one axis should zoom in all axes – I know this is possible) (planned: docs/plan/PLAN_api-v2.md, phase 2)
[] Some sort of indication that “I am working on it…” when data is being updated (Low priority, now that we have fast updates) (planned: docs/plan/PLAN_api-v2.md, phase 2)
[x] Graph of battery voltage (Maybe move to less prominent location?)

## Notes
- ***2025-08-29:*** Required packages in requirements.txt are not version number locked. An automatic rebuild in connection with an ansible relaunch of the server, must have caused upgrading of some packages that introduced new behavior. The app now by default runs in async mode, which is incompatible with calls to the Django ORM. Fixed by wrapping all calls to `db_connector` functions that access the Django ORM with a call to `asgiref.sync.sync_to_async()`. This problem stresses the need to keep all Django ORM access separate in the `db_connector` module, for better mainainability.
- ***2026-11-15:*** The app is being moved from the monitoring_db API v1 to v2 (docs/plan/PLAN_api-v2.md). Phase 1 keeps the page and adds a logger selector (Tekbox / Campbell CR350 per mast); a new page will be developed side by side at /app/nuki/v2/.

## Release build and tests

Releases are built from a tag on `develop/main` by `scripts/build_image.sh` in dtu_sus_data02
(`scripts/build_image.sh webapp_nuki https://github.com/tingeman/webapp_nukissiorfiit.git <tag>`).
It runs a plain `docker build` of the checkout and then `bash tests/run.sh <image>`; no bundle
is written unless the tests pass.

### Dependencies (lock file)

- Direct dependencies: `app/requirements.in` (production), `app/requirements-dev.in`
  (ipython, ipdb, matplotlib-inline; not in the image).
- Locks: `app/requirements.txt` and `app/requirements-dev.txt`, all transitive packages with
  hashes. The Dockerfile installs `requirements.txt` with `pip install --require-hashes`.
- Compile them after editing an `.in` file (needs Docker and Python 3.8+ on the workstation;
  uv runs in a container of the pinned Python image, from `compose.pins.env`):

```bash
python scripts/compile_lock.py                    # keep the locked versions, add/remove what changed
python scripts/compile_lock.py --package dash     # upgrade one package (repeatable)
python scripts/compile_lock.py --upgrade --exclude-newer 2026-11-01   # upgrade all, with a cooldown
```

- Base image: `ARG PYTHON_IMAGE` in the Dockerfile and `NUKI_PYTHON_IMAGE` in
  `compose.pins.env` hold the same `python:<version>-slim@sha256:<digest>` (a test checks
  it). To update, change both (`docker buildx imagetools inspect python:3.13-slim`) and
  recompile the locks.

### Build and test locally

```bash
docker build -t webapp_nuki:local .
bash tests/run.sh webapp_nuki:local                            # full suite (what the release build runs)
TEST_FILTER='^unit:' bash tests/run.sh webapp_nuki:local       # only the unit tests
bash tests/run.sh --quick webapp_nuki:local                    # skip the smoke tests (container start)
APP_SRC=app bash tests/run.sh webapp_nuki:local                # test the checkout's app/ without rebuilding
```

The tests use a stub of the monitoring_db API (`tests/fake_api.py`, fixtures in
`tests/fixtures/`) and a dummy env file; no secrets, no network. See the header of
`tests/run.sh`. The env variables the app reads are documented in `webapp_nuki.env.example`.

### Development (dev stage)

The Dockerfile has three stages: `runtime` (the production image), `dev` (runtime plus
IPython/ipdb from `app/requirements-dev.txt`, gunicorn with `--reload`, one worker,
`--timeout 0`) and `prod` (= runtime). Releases run a plain `docker build`, which builds the
last stage, `prod`, only: the dev tools never reach a release, and `prod` must stay last.

```bash
docker compose -f compose.develop.yml up --build webapp_nuki      # build the dev stage and start
docker build --target dev -t webapp_nuki:dev .                    # or build the dev image alone
```

- Code reload: `./app` is mounted over `/app`, so saving a `.py` file restarts the worker
  (gunicorn polls the files; works on bind mounts from Windows). No rebuild needed.
- ipdb: put `import ipdb; ipdb.set_trace()` in the code, trigger it in the browser, then
  `docker attach webapp-nuki` to get the prompt (the service has `stdin_open` and `tty`).
  Detach with Ctrl-P Ctrl-Q; Ctrl-C would stop the container. `--timeout 0` keeps gunicorn
  from killing the worker while it waits at a breakpoint.
- Rebuild (`--build`) after a change to `app/requirements*.txt` or the Dockerfile.
- The env file comes from `secrets/webapp_nuki.dev.env` (mounted as
  `/app/secrets/.env.development`); docker creates the empty mount point `app/secrets/` in
  the checkout, which git ignores.

## Third-party files

- `app/v2/assets/bootstrap.min.css` (phase 2 page, `/app/nuki/v2/`): Bootstrap **5.3.8**,
  unchanged, copied byte for byte from webapp_grl_tsp `app/assets/bootstrap.min.css`, which
  was downloaded from `https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/css/bootstrap.min.css`
  (2026-11-10, 232111 bytes, MIT licence in its header). Integrity
  `sha384-sRIl4kxILFvY47J16cr9ZwB07vP4J8+LH7qKQnuqkuIAvNWLzeN8tE5YBujZqJLB` (the SRI
  published by getbootstrap.com for 5.3.8), sha256
  `d85327d99c7a3ee1f9b5d0500d1370acea3ad2db39c163c2f51f232baedbdede` (equal to jsDelivr's
  package listing and to webapp_grl_tsp's copy; a unit test checks it). `.gitattributes`
  keeps it byte-identical (`-text`). Served by the phase 2 Dash app itself under
  `/app/nuki/v2/assets/`, so the page needs no CDN; the classic page does not load it. To
  update: take the new file from webapp_grl_tsp (or download it and check its hash against
  the published SRI), replace the file and update this paragraph, the hash in
  `tests/unit/test_v2_page.py` and `app/v2/assets/style.css`'s header.
  dash-bootstrap-components 2.0 supports Bootstrap 5.3.
- plotly.js and the Dash renderer come from the installed Python packages (`serve_locally`).

## Installation

1) Clone repository from github
2) Install required modules:
```bash
python -m pip install -r requirements.txt
```

## Usage

### Current Recommended Local Startup (Docker)

From repo root:

```powershell
docker compose --project-name nuki -f .\compose.develop.yml -f .\compose.develop.local.yml up -d --build webapp_nuki
```

Open app at:

`http://localhost:8050/app/nuki/`

Optional: start nginx too (for localhost:80):

```powershell
docker compose --project-name nuki -f .\compose.develop.yml -f .\compose.develop.local.yml up -d --build nginx webapp_nuki
```

### Direct Python Run (non-docker)

Run development server:
```bash
python ./app/app.py
```
Navigate to web page `127.0.0.1:8050/app/nuki/`.

