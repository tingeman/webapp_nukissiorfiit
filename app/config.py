import re

from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from dotenv import load_dotenv
from pathlib import Path
import os

# set up logging to console
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConfigSettings(BaseSettings):
    DEBUG: bool = Field(..., env="DEBUG")

    influxdb_url: str = Field(..., env="INFLUXDB_URL")
    influxdb_token: str = Field(..., env="INFLUXDB_TOKEN")
    influxdb_org: str = Field(..., env="INFLUXDB_ORG")
    # Root of the monitoring_db API (a trailing /api/v1 or /api/v2 is stripped by db_connector)
    backend_api_base_url: str = Field("http://host.docker.internal:8099", env="BACKEND_API_BASE_URL")
    backend_api_timeout_seconds: float = Field(30.0, env="BACKEND_API_TIMEOUT_SECONDS")
    # Retries of connection errors only (a 504 or a read timeout is never retried)
    backend_api_max_retries: int = Field(2, env="BACKEND_API_MAX_RETRIES")
    backend_api_backoff_factor: float = Field(0.5, env="BACKEND_API_BACKOFF_FACTOR")
    # Most /raw requests in flight at once per data call, and the years per /raw request
    backend_api_max_parallel: int = Field(2, gt=0, env="BACKEND_API_MAX_PARALLEL")
    backend_api_raw_chunk_years: int = Field(1, gt=0, env="BACKEND_API_RAW_CHUNK_YEARS")
    # Phase 2 page (app/v2/): hydrological years (from 1 August) per /daily request
    backend_api_daily_chunk_years: int = Field(3, gt=0, env="BACKEND_API_DAILY_CHUNK_YEARS")
    # Phase 2 download: no new API request is started after this many seconds (must stay
    # below nginx's proxy_read_timeout minus BACKEND_API_TIMEOUT_SECONDS and the build time)
    download_time_budget_s: float = Field(75.0, gt=0, env="DOWNLOAD_TIME_BUDGET_S")
    # Stations whose identifier fully matches this regular expression are shown
    nuki_station_pattern: str = Field(r"^(SIS2024-0[1-5]|NUKI202[0-9]-.+)$", env="NUKI_STATION_PATTERN")

    @field_validator("nuki_station_pattern")
    @classmethod
    def _check_station_pattern(cls, value):
        try:
            re.compile(value)
        except re.error as exc:
            raise ValueError(f"NUKI_STATION_PATTERN is not a valid regular expression ({exc}): {value!r}") from None
        return value

    @property
    def station_regex(self):
        """nuki_station_pattern compiled (use with fullmatch)."""
        return re.compile(self.nuki_station_pattern)

    class Config:
        env_file = ".env"


def load_environment():
    env = os.getenv('APP_ENV', 'development')
    env_file = f".env.{env}"

    # get the path of this module, independently of the current working directory, using pathlib
    module_path = Path(__file__).parent

    logger.debug(f"Loading environment from {env_file} file...")
    
    if not (module_path / 'secrets' / env_file).exists():
        if (module_path / ".env.example").exists():
            logger.warning(f"No {env_file} file found. Creating one from .env.example...")
            with open(module_path / ".env.example") as f:
                example_content = f.read()
            with open(module_path / 'secrets' / env_file, 'w') as f:
                f.write(example_content)
            logger.warning(f"Please update the {env_file} file with your configuration.")
        else:
            raise FileNotFoundError(f"No {env_file} or .env.example file found. Please create one.")

    load_dotenv(module_path / 'secrets' / env_file, override=True)

load_environment()
settings = ConfigSettings()
