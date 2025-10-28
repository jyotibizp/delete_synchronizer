from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import os

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # dotenv is optional in some environments
    pass


@dataclass(frozen=True)
class Settings:
    azure_storage_connection_string: str
    azure_blob_container: str
    
    environment: str
    
    snowflake_account: str
    snowflake_user: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_table: str    
    snowflake_private_key_path: str


_settings: Settings | None = None


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def get_settings() -> Settings:
    global _settings
    if _settings is not None:
        return _settings

    _settings = Settings(
        azure_storage_connection_string=_env("AZURE_STORAGE_CONNECTION_STRING"),
        azure_blob_container=_env("AZURE_BLOB_CONTAINER", "events"),
        environment=_env("ENVIRONMENT", "local").lower(),
        snowflake_account=_env("SNOWFLAKE_ACCOUNT"),
        snowflake_user=_env("SNOWFLAKE_USER"),
        snowflake_warehouse=_env("SNOWFLAKE_WAREHOUSE"),
        snowflake_database=_env("SNOWFLAKE_DATABASE"),
        snowflake_schema=_env("SNOWFLAKE_SCHEMA", "PUBLIC"),
        snowflake_table=_env("SNOWFLAKE_TABLE", "delete_tracker"),
        snowflake_private_key_path=_env("SNOWFLAKE_PRIVATE_KEY_PATH"),
    )

    return _settings

