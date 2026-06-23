# webapp_nukissiorfiit

## TODO list
[] Date range does not seem to go beyond 2025-01-30?? This seems to be related to session persistence. Fix to update max_end_date dynamically attempted (is it fixed?)
[x] Filtering of data should include all datapoints on the last date of th  e daterange 
[] New graph with temperature as a function of depth. A slider should allow user to iterate through the entire date range. Should plot the daily average at each sensor depth.
[] Something is wrong with the table of measurements, it doesn't show timestamps
[] Inclinations in 3 separate but linked axes, so that variations are visible (individual scales)
[] Map insert with location of station (grab location from station table in database)
[] Photo of station (we will have to discuss how to best implement this)
[] Remove old code that interacts with influxdb
[] Link the zooming in all axes (zoom in one axis should zoom in all axes – I know this is possible)
[] Some sort of indication that “I am working on it…” when data is being updated (Low priority, now that we have fast updates)
[x] Graph of battery voltage (Maybe move to less prominent location?)

## Notes
- ***2025-08-29:*** Required packages in requirements.txt are not version number locked. An automatic rebuild in connection with an ansible relaunch of the server, must have caused upgrading of some packages that introduced new behavior. The app now by default runs in async mode, which is incompatible with calls to the Django ORM. Fixed by wrapping all calls to `db_connector` functions that access the Django ORM with a call to `asgiref.sync.sync_to_async()`. This problem stresses the need to keep all Django ORM access separate in the `db_connector` module, for better mainainability.

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

