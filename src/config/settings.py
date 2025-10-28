from __future__ import annotations

from dataclasses import dataclass
import os
from typing import List, Optional

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # dotenv is optional in some environments
    pass


@dataclass(frozen=True)
class Settings:
    sf_client_id: str
    sf_username: str
    sf_login_url: str
    sf_audience: str
    sf_private_key_path: str
    sf_topic_names: List[str]
    
    snowflake_account: str
    snowflake_user: str
    snowflake_private_key_path: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_table: str
    
    mock_mode: bool
    mock_data_dir: str


_settings: Settings | None = None


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def get_settings() -> Settings:
    global _settings
    if _settings is not None:
        return _settings

    topic_names_csv = _env("SF_TOPIC_NAMES", "/event/Delete_Logs__e")
    topic_names = [t.strip() for t in topic_names_csv.split(",") if t.strip()]

    _settings = Settings(
        sf_client_id=_env("SF_CLIENT_ID"),
        sf_username=_env("SF_USERNAME"),
        sf_login_url=_env("SF_LOGIN_URL", "https://login.salesforce.com"),
        sf_audience=_env("SF_AUDIENCE", "https://login.salesforce.com"),
        sf_private_key_path=_env("SF_PRIVATE_KEY_PATH", "certs/private.key"),
        sf_topic_names=topic_names,
        snowflake_account=_env("SNOWFLAKE_ACCOUNT"),
        snowflake_user=_env("SNOWFLAKE_USER"),
        snowflake_private_key_path=_env("SNOWFLAKE_PRIVATE_KEY_PATH", "certs/rsa_key.p8"),
        snowflake_warehouse=_env("SNOWFLAKE_WAREHOUSE"),
        snowflake_database=_env("SNOWFLAKE_DATABASE"),
        snowflake_schema=_env("SNOWFLAKE_SCHEMA", "PUBLIC"),
        snowflake_table=_env("SNOWFLAKE_TABLE", "delete_tracker"),
        mock_mode=_env("MOCK_MODE", "false").lower() in ("true", "1", "yes"),
        mock_data_dir=_env("MOCK_DATA_DIR", "mock_data"),
    )

    return _settings

