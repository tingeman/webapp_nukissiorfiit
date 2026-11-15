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
PERIOD = "Period"
RESOLUTION = "Values"
RES_DAILY = "Daily"
RES_FULL = "Full"
RESOLUTION_HELP = ("Daily: daily mean with the day's minimum and maximum. Full: every "
                   "measurement, at most one year; zoom in for full detail.")

# ---- messages -------------------------------------------------------------------------------
MASTS_UNAVAILABLE = "The list of masts could not be loaded. {reason}"
NO_MASTS = "No mast with data was found in the database."
CHOOSE_FIRST = "Choose a mast and a logger."
CHOOSE_PERIOD = "Choose a start and an end date."
PERIOD_REVERSED = "The start date is after the end date."
NO_DATA = "No data for this logger between {start} and {end}."
UNEXPECTED = "Something went wrong while preparing the plots. Please try again."

# ---- info line ------------------------------------------------------------------------------
# what the figure shows
INFO_DAILY = "Daily means with daily minimum and maximum, {start} to {end}"
INFO_FULL = "Every measurement, {start} to {end}"
INFO_THINNED = ("{start} to {end}, thinned to the minimum and maximum per ~{bin} "
                "(zoom in for every measurement)")
INFO_ZOOM = "Zoomed to {start} to {end}"
# a Full range longer than one year loads the year up to its end date
INFO_FULL_LIMIT = ("Full resolution is limited to one year: showing the last year "
                   "of the chosen period.")
# the API part: {requests} count, {seconds} wall time, {size} e.g. "12 KB"
INFO_REQUESTS = "{requests} request(s), {seconds:.2f} s, {size}"
INFO_DETAILS = "Requests"
INFO_REQUEST_LINE = "{start} to {end}: {seconds:.2f} s, {size}"

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
# {name} the series label, {unit} its unit (may be empty)
HOVER_DAILY = ("%{{x|%Y-%m-%d}}<br>mean %{{y:.3f}} {unit}<br>"
               "min %{{customdata[0]:.3f}} · max %{{customdata[1]:.3f}}<extra>{name}</extra>")
HOVER_RAW = "%{{x|%Y-%m-%d %H:%M:%S}} UTC<br>%{{y:.3f}} {unit}<extra>{name}</extra>"

# ---- API problems (api_client.ApiError: str(error) is one of these) -----------------------
# {detail} is the API's own message
API_UNREACHABLE = "The data service cannot be reached right now. Please try again later."
API_TIMEOUT = ("The data service did not answer in time. Try a shorter period, "
               "or try again later.")
API_ERROR = "The data service answered with an error: {detail}"
API_INVALID = "The data service sent an answer that cannot be read. Please try again later."
# the download's time budget (DOWNLOAD_TIME_BUDGET_S) ran out before all requests were started
DOWNLOAD_TOO_LARGE = "Download too large; choose fewer years or loggers"
