"""Validated configuration. Secret files take precedence; never log their contents."""

from __future__ import annotations

import os
import sys
from importlib import resources
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

ALLOWED_HOSTS = frozenset(
    {"api-endpoint.mta.info", "rrgtfsfeeds.s3.amazonaws.com", "api.weather.gov", "www.ncei.noaa.gov"}
)


def configure_spark_python():
    """A venv driver must not silently start workers with the system Python."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def secret(name: str, default: str = "") -> str:
    filename = os.getenv(name + "_FILE")
    if filename:
        return Path(filename).read_text().strip()
    return os.getenv(name, default)


def trusted_url(value: str) -> str:
    p = urlparse(value)
    if (
        p.scheme != "https"
        or p.hostname not in ALLOWED_HOSTS
        or p.username
        or p.password
        or p.port not in (None, 443)
    ):
        raise ValueError("Source must use HTTPS and an approved public hostname")
    return value


@dataclass
class Settings:
    data_dir: Path = field(default_factory=lambda: Path(os.getenv("DATA_DIR", "data")))
    mode: str = field(default_factory=lambda: os.getenv("DATA_MODE", "live"))
    environment: str = field(default_factory=lambda: os.getenv("APP_ENV", "development"))
    database_url: str = field(default_factory=lambda: secret("DATABASE_URL"))
    api_token: str = field(default_factory=lambda: secret("API_TOKEN"))
    kafka_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""))
    routes: tuple[str, ...] = ("1", "2", "3", "4", "5", "6")
    timezone: str = "America/New_York"
    stale_seconds: int = 120
    lateness_seconds: int = 600
    poll_seconds: int = 30
    static_url: str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip"
    realtime_url: str = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
    alerts_url: str = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts"
    weather_url: str = "https://api.weather.gov/stations/KNYC/observations/latest"

    def __post_init__(self):
        if self.mode not in {"live", "synthetic", "recorded"}:
            raise ValueError("Invalid DATA_MODE")
        if self.environment not in {"development", "test", "production"}:
            raise ValueError("Invalid APP_ENV")
        if self.environment == "production" and not self.database_url.startswith("postgresql"):
            raise ValueError(
                "Production requires PostgreSQL DATABASE_URL; serving API validates its own API_TOKEN"
            )
        if not self.routes or any(r not in {"1", "2", "3", "4", "5", "6"} for r in self.routes):
            raise ValueError("Unsupported route scope")
        for value in (self.static_url, self.realtime_url, self.alerts_url, self.weather_url):
            trusted_url(value)
        if any(
            not isinstance(v, int) or isinstance(v, bool)
            for v in (self.poll_seconds, self.stale_seconds, self.lateness_seconds)
        ):
            raise ValueError("Window settings must be integer seconds")
        if self.poll_seconds < 15 or self.stale_seconds < 1 or self.lateness_seconds < 1:
            raise ValueError("Invalid collection/window settings")

    @property
    def event_topics(self) -> dict[str, str]:
        from .event_topics import EVENT_TYPES, event_topic
        return {event_type: event_topic(self.mode, event_type) for event_type in EVENT_TYPES}

    def topic_for(self, event_type: str) -> str:
        from .event_topics import event_topic
        return event_topic(self.mode, event_type)

    @property
    def transit_topic(self) -> str:
        return self.topic_for("transit-events")

    @classmethod
    def from_yaml(cls, path: str | Path):
        import yaml

        content = yaml.safe_load(Path(path).read_text())
        if not isinstance(content, dict):
            raise ValueError("Source configuration must be a mapping")
        allowed = {
            "routes",
            "timezone",
            "stale_seconds",
            "lateness_seconds",
            "poll_seconds",
            "static_url",
            "realtime_url",
            "alerts_url",
            "weather_url",
        }
        if set(content) - allowed:
            raise ValueError("Unknown source configuration setting")
        if "routes" in content:
            content["routes"] = tuple(str(v) for v in content["routes"])
        return cls(**content)

    @classmethod
    def load(cls):
        requested = os.getenv("SOURCE_CONFIG")
        if requested:
            return cls.from_yaml(Path(requested))
        default = resources.files("data_horizon").joinpath("resources").joinpath("nyc.yaml")
        with resources.as_file(default) as path:
            return cls.from_yaml(path)
