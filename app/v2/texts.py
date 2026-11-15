"""Texts of the phase 2 page (one place for wording, as in webapp_grl_tsp/app/texts.py).

Templates use str.format fields; the field names are part of the interface. Plotly hover
templates double the braces of their own %{...} fields.
"""

# ---- page -----------------------------------------------------------------------------------
# same H1 as the classic page (app.py)
TITLE = "Structural Health Monitoring Power Line Masts Kangerluarsuk Ungalleq"
PAGE_TITLE = "Nukissiorfiit masts (new page)"
SUBTITLE = "Nukissiorfiit monitoring stations · DTU Sustain"
BADGE = "New page (in development)"
INTRO = ("This page is under development. It shows the data of the Nukissiorfiit "
         "monitoring stations in a new layout. Until it is finished, the classic page "
         "remains available.")
BACK_LINK = "Back to the classic page"
FOOTER = ("Data: DTU Sustain monitoring database. Times are UTC; daily values are per UTC "
          "day.")

# ---- controls -------------------------------------------------------------------------------
MAST = "Mast"
MAST_PLACEHOLDER = "Choose a mast"
LOGGER = "Logger"
LOGGER_PLACEHOLDER = "Choose a logger"
# the last option of the logger dropdown when the mast has 2 or more loggers (T11)
LOGGER_ALL = "All loggers"
PERIOD = "Period"
RESOLUTION = "Values"
RES_DAILY = "Daily"
RES_FULL = "Full"
RESOLUTION_HELP = ("Daily: the daily mean. Full: every measurement, at most one year; zoom "
                   "in for full detail. A zoom is kept when you switch.")

# ---- messages -------------------------------------------------------------------------------
MASTS_UNAVAILABLE = "The list of masts could not be loaded. {reason}"
NO_MASTS = "No mast with data was found in the database."
CHOOSE_FIRST = "Choose a mast and a logger."
CHOOSE_PERIOD = "Choose a start and an end date."
PERIOD_REVERSED = "The start date is after the end date."
NO_DATA = "No data for this logger between {start} and {end}."
NO_DATA_ALL = "No data for any logger of this mast between {start} and {end}."
UNEXPECTED = "Something went wrong while preparing the plots. Please try again."

# ---- info line ------------------------------------------------------------------------------
# what the figure shows
INFO_DAILY = "Daily means, {start} to {end}"
INFO_FULL = "Every measurement, {start} to {end}"
INFO_THINNED = ("{start} to {end}, thinned to the minimum and maximum per ~{bin} "
                "(zoom in for every measurement)")
INFO_ZOOM = "Zoomed to {start} to {end}"
# a Full range longer than one year loads the year up to its end date
INFO_FULL_LIMIT = ("Full resolution is limited to one year: showing the last year "
                   "of the chosen period.")
# the Daily/Full switch keeps a zoom: a zoomed window longer than one year loads its last year
INFO_ZOOM_LIMIT = ("Full resolution is limited to one year: showing the last year "
                   "of the zoomed window.")
# the API part: {requests} count, {seconds} wall time, {size} e.g. "12 KB"
INFO_REQUESTS = "{requests} request(s), {seconds:.2f} s, {size}"
INFO_DETAILS = "Requests"
INFO_REQUEST_LINE = "{start} to {end}: {seconds:.2f} s, {size}"
# Full: the kind of each request ({kind} is INFO_KIND_RAW or INFO_KIND_PROFILE) before the line
INFO_REQUEST_KIND = "{kind}: "
INFO_KIND_RAW = "raw"
INFO_KIND_PROFILE = "daily, for the profile"
# All loggers: the loggers fetched ({loggers}: their names, joined by "; "), and each
# request line starts with its logger's name ("{kind}" above is then "<logger>, <kind>")
INFO_LOGGERS = "Loggers: {loggers}"
INFO_NO_LOGGER = "no logger has data in this period"

# ---- heading of the figure card: what is drawn (T16) --------------------------------------
# the parts, joined by HEADING_SEP: mast, station(s), logger (or HEADING_ALL), the values
# (HEADING_DAILY / HEADING_FULL) and the period (HEADING_PERIOD)
HEADING_SEP = " · "
HEADING_ALL = "All loggers: {loggers}"
HEADING_DAILY = "Daily means"
HEADING_FULL = "Full resolution"
HEADING_PERIOD = "{start} to {end}"
# after the selection when nothing is drawn for it
HEADING_NO_DATA = "no data"
HEADING_FAILED = "not loaded: {reason}"

# ---- figure ---------------------------------------------------------------------------------
FIGURE_EMPTY = "Choose a mast and a logger to see its data."
ROW_GROUND = "Ground temperature"
ROW_AIR = "Air temperature"
ROW_HUMIDITY = "Relative humidity"
ROW_PRESSURE = "Barometric pressure"
ROW_INCLINATION = "Inclination {name}"
ROW_HEADING = "Compass heading"
ROW_SENSOR_TEMPERATURE = "Sensor temperature"
ROW_BATTERY = "Battery voltage"
# the row title says what is shown ({title} one of the ROW_ texts above; {bin} e.g. "6 h")
ROW_DAILY = "{title}, daily mean"
ROW_FULL = "{title}, all measurements"
ROW_THINNED = "{title}, thinned (min/max per ~{bin})"
# {name} the series label, {unit} its unit (may be empty)
HOVER_DAILY = ("%{{x|%Y-%m-%d}}<br>mean %{{y:.3f}} {unit}<br>"
               "min %{{customdata[0]:.3f}} · max %{{customdata[1]:.3f}}<extra>{name}</extra>")
# All loggers with several stations (Mast 9): a trace's hover name ({name} the series label)
TRACE_STATION = "{name} ({station})"
# the label at a logger change (dashed line): {logger} the logger's name
CHANGE_LABEL = "{logger}"
HOVER_RAW = "%{{x|%Y-%m-%d %H:%M:%S}} UTC<br>%{{y:.3f}} {unit}<extra>{name}</extra>"

# ---- ground temperature profile (T10) ------------------------------------------------------
PROFILE_TITLE = "Ground temperature profile"
PROFILE_DAY = "Day"
PROFILE_ENVELOPE = "Envelope"
ENV_DAY = "Day min/max"
ENV_ANNUAL = "Annual envelope"
ENV_BOTH = "Both"
ENVELOPE_HELP = ("Day min/max: the lowest and highest value of the chosen day at each depth. "
                 "Annual envelope: the lowest and highest daily mean of the 365 days up to "
                 "the chosen day.")
PROFILE_EMPTY = "The profile appears here when a logger's data is loaded."
PROFILE_NO_GROUND = "This logger has no ground temperature data in the chosen period, so there is no profile."
PROFILE_ERROR = "The data for the profile could not be loaded. {reason}"
PROFILE_NO_DAY = "No ground temperature data on {day}."
# the line above the profile: {day}; the annual part only with an annual envelope
PROFILE_LABEL = "Daily mean of {day}"
PROFILE_LABEL_ANNUAL = ("annual envelope from the daily means of {start} to {end} "
                        "({valid} of {days} days with data)")
PROFILE_LABEL_SHORT = " (the loaded data starts on {start})"
# All loggers with several stations: the station the profile is from
PROFILE_LABEL_STATION = ", station {station}"
PROFILE_MEAN = "Daily mean"
PROFILE_DAY_BAND = "Day min/max"
PROFILE_ANNUAL_MAX = "Annual max"
PROFILE_ANNUAL_MIN = "Annual min"
PROFILE_XAXIS = "Temperature (°C)"
PROFILE_YAXIS = "Depth (m)"
# {name} the trace name; customdata is the depth label
HOVER_PROFILE = "%{{customdata}}: %{{x:.2f}} °C<extra>{name}</extra>"
# the daily mean, with the day's min and max (customdata: label, min, max)
HOVER_PROFILE_MEAN = ("%{{customdata[0]}}: %{{x:.2f}} °C<br>"
                      "day min %{{customdata[1]:.2f}} · max %{{customdata[2]:.2f}}<extra>{name}</extra>")

# ---- API problems (api_client.ApiError: str(error) is one of these) -----------------------
# {detail} is the API's own message
API_UNREACHABLE = "The data service cannot be reached right now. Please try again later."
API_TIMEOUT = ("The data service did not answer in time. Try a shorter period, "
               "or try again later.")
API_ERROR = "The data service answered with an error: {detail}"
API_INVALID = "The data service sent an answer that cannot be read. Please try again later."
# the download's time budget (DOWNLOAD_TIME_BUDGET_S) ran out before all requests were started
DOWNLOAD_TOO_LARGE = "Download too large; choose fewer years or loggers"

# ---- download (T12) -------------------------------------------------------------------------
DOWNLOAD_TITLE = "Download"
DOWNLOAD_INTRO = ("An Excel workbook with every measurement (one \"full\" sheet per logger) and "
                  "the daily mean, minimum, maximum and count (one \"daily\" sheet per logger) "
                  "of the chosen loggers and calendar years (UTC). Large downloads take a while.")
DOWNLOAD_FROM = "From year"
DOWNLOAD_TO = "To year"
DOWNLOAD_LOGGERS = "Loggers"
DOWNLOAD_ALL = "Toggle all recording devices at this station"
DOWNLOAD_BUTTON = "Download Excel file"
DOWNLOAD_RUNNING = "Preparing the file..."
DOWNLOAD_CHOOSE_YEARS = "Choose the years to download."
DOWNLOAD_YEARS_REVERSED = "The first year is after the last year."
DOWNLOAD_CHOOSE_LOGGERS = "Choose at least one logger to download."
DOWNLOAD_NO_DATA = "The chosen loggers have no data from {start} to {end}."
# the message after a download: {filename}, {size} e.g. "120 KB"
DOWNLOAD_DONE = "Downloaded {filename} ({size})."

# ---- the workbook (download.py) --------------------------------------------------------------
XLSX_TITLE = "Nukissiorfiit monitoring stations: data export (DTU Sustain)"
XLSX_MAST = "Mast"
XLSX_YEARS = "Requested years"
XLSX_YEARS_VALUE = "{start} to {end} (calendar years, UTC)"
XLSX_DOWNLOADED = "Downloaded (UTC)"
XLSX_SOURCE = "Source"
XLSX_SOURCE_VALUE = ("DTU Sustain monitoring database, read API v2 (/measurements/raw and "
                     "/measurements/daily), with the API's defaults: calibrated values, masked "
                     "values left out")
XLSX_NOTE = "Note"
XLSX_NOTES = (
    "All times are UTC (Excel datetimes without a time zone); daily values are per UTC day.",
    "Values are in the units stored in the database (column details below). Unlike the web "
    "page, barometric pressure is not converted to kPa.",
    "Daily sheets: mean, minimum, maximum and number of measurements per day, as computed "
    "by the API (the same numbers as the web page's Daily view).",
)
XLSX_LOGGERS = "Loggers"
XLSX_LOGGER_HEADERS = ("Logger", "Label", "Selector", "Station", "First measurement (UTC)",
                       "Last measurement (UTC)", "Days exported (UTC)", "Sheets")
XLSX_DAYS = "{start} to {end}"
XLSX_NOT_EXPORTED = "no data in the requested years (not exported)"
XLSX_COLUMNS = "Column details"
XLSX_COLUMN_HEADERS = ("Sheet", "Column", "Header", "Sensor", "Series", "Unit",
                       "Depth / relative_z (m)", "Datatype", "Sensor type", "API key", "Note")
XLSX_TIME = "Time (UTC)"
XLSX_DATE = "Date (UTC)"
XLSX_TIME_NOTE = "Measurement time, UTC"
XLSX_DATE_NOTE = "UTC day"
XLSX_STAT = {"mean": "Daily mean.", "min": "Daily minimum.", "max": "Daily maximum.",
             "count": "Number of measurements of the day."}
XLSX_NOTE_PRESSURE = "In {unit} as stored (not converted to kPa)."
