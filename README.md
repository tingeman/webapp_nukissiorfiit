# webapp_nukissiorfiit

## TODO list
[] Date range does not seem to go beyond 2025-01-30?? This seems to be related to session persistence. Fix to update max_end_date dynamically attempted (is it fixed?)
[x] Filtering of data should include all datapoints on the last date of the daterange 
[] New graph with temperature as a function of depth. A slider should allow user to iterate through the entire date range. Should plot the daily average at each sensor depth.
[] Something is wrong with the table of measurements, it doesn't show timestamps
[] Inclinations in 3 separate but linked axes, so that variations are visible (individual scales)
[] Map insert with location of station (grab location from station table in database)
[] Photo of station (we will have to discuss how to best implement this)
[] Remove old code that interacts with influxdb
[] Link the zooming in all axes (zoom in one axis should zoom in all axes – I know this is possible)
[] Some sort of indication that “I am working on it…” when data is being updated (Low priority, now that we have fast updates)
[x] Graph of battery voltage (Maybe move to less prominent location?)



## Installation

1) Clone repository from github
2) Install required modules:
```bash
python -m pip install -r requirements.txt
```

## Usage
Run development server:
```bash
python ./app/app.py
```
Navigate to web page `127.0.0.1:8050`.

