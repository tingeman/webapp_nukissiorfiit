from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    influxdb_url: str
    influxdb_token: str
    influxdb_org: str
    debug_mode: bool = True

    class Config:
        env_file = ".env"

settings = Settings()
