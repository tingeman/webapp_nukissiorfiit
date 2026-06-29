from pydantic_settings import BaseSettings
from pydantic import Field
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
    backend_api_base_url: str = Field("http://host.docker.internal:8099/api/v1", env="BACKEND_API_BASE_URL")
    backend_api_timeout_seconds: float = Field(30.0, env="BACKEND_API_TIMEOUT_SECONDS")
    backend_api_max_retries: int = Field(2, env="BACKEND_API_MAX_RETRIES")
    backend_api_backoff_factor: float = Field(0.5, env="BACKEND_API_BACKOFF_FACTOR")

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
