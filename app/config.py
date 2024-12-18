from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv
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

    class Config:
        env_file = ".env"


def load_environment():
    env = os.getenv('APP_ENV', 'development')
    env_file = f".env.{env}"
    logger.debug(f"Loading environment from {env_file} file...")
    
    if not os.path.exists(env_file):
        if os.path.exists(".env.example"):
            logger.debug(f"No {env_file} file found. Creating one from .env.example...")
            with open(".env.example") as f:
                example_content = f.read()
            with open(env_file, 'w') as f:
                f.write(example_content)
            logger.debug(f"Please update the {env_file} file with your configuration.")
        else:
            raise FileNotFoundError(f"No {env_file} or .env.example file found. Please create one.")

    load_dotenv(env_file, override=True)

load_environment()
settings = ConfigSettings()
