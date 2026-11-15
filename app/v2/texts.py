"""Texts of the phase 2 page (one place for wording, as in webapp_grl_tsp/app/texts.py).

Templates use str.format fields; the field names are part of the interface.
"""

# same H1 as the classic page (app.py)
TITLE = "Structural Health Monitoring Power Line Masts Kangerluarsuk Ungalleq"
PAGE_TITLE = "Nukissiorfiit masts (new page)"
BADGE = "New page (in development)"
INTRO = ("This page is under development. It will show the data of the Nukissiorfiit "
         "monitoring stations in a new layout. Until it is finished, use the classic page.")
BACK_LINK = "Back to the classic page"

# ---- API problems (api_client.ApiError: str(error) is one of these) -----------------------
# {detail} is the API's own message
API_UNREACHABLE = "The data service cannot be reached right now. Please try again later."
API_TIMEOUT = ("The data service did not answer in time. Try a shorter period, "
               "or try again later.")
API_ERROR = "The data service answered with an error: {detail}"
API_INVALID = "The data service sent an answer that cannot be read. Please try again later."
# the download's time budget (DOWNLOAD_TIME_BUDGET_S) ran out before all requests were started
DOWNLOAD_TOO_LARGE = "Download too large; choose fewer years or loggers"
