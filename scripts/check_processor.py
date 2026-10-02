"""Container liveness check; source freshness is reported separately by the API."""
from pathlib import Path
import os
import time

marker = Path(os.getenv("DATA_DIR", "/data")) / "processor-heartbeat"
if not marker.exists() or time.time() - marker.stat().st_mtime > 180:
    raise SystemExit(1)
