import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str
    
    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        required_vars = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER", 
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA",
            "SNOWFLAKE_ROLE"
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        return cls(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )


@dataclass
class FMPConfig:
    api_key: str
    base_url: str
    rate_limit_calls: int
    rate_limit_period: int
    
    @classmethod
    def from_env(cls) -> "FMPConfig":
        api_key = os.getenv("FMP_API_KEY")
        if not api_key:
            raise ValueError("Missing required environment variable: FMP_API_KEY")
        
        return cls(
            api_key=api_key,
            base_url=os.getenv("FMP_BASE_URL", "https://financialmodelingprep.com/api/v3"),
            rate_limit_calls=int(os.getenv("RATE_LIMIT_CALLS", "300")),
            rate_limit_period=int(os.getenv("RATE_LIMIT_PERIOD", "60"))
        )


@dataclass
class AppConfig:
    log_level: str
    batch_size: int
    
    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            batch_size=int(os.getenv("BATCH_SIZE", "1000"))
        )


class Config:
    def __init__(self):
        self.snowflake = SnowflakeConfig.from_env()
        self.fmp = FMPConfig.from_env()
        self.app = AppConfig.from_env()
        
        logger.level = self.app.log_level
        logger.info("Configuration loaded successfully")
        
    @classmethod
    def load(cls) -> "Config":
        return cls()