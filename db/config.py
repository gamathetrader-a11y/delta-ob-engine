"""Configuration settings and precomputed lookup tables for Delta Exchange India Live Chart"""

import re

SYMBOLS = ["ETHUSD", "BTCUSD", "PAXGUSD", "SLVONUSD"]
SYMBOL = SYMBOLS  # Alias for compatibility
DEFAULT_SYMBOL = "BTCUSD"

# Native resolutions fetched directly from Delta Exchange REST & WebSocket
RESOLUTIONS = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "1d", "1w"]

# Complete list of resolutions available in the UI dropdown (Native + Resampled)
UI_RESOLUTIONS = [
    "1m", "3m", "5m", "10m", "15m", "30m", "45m",
    "1h", "2h", "4h", "6h", "1d", "1w",
    "1M", "3M", "6M", "12M"
]

# Maps custom resampled resolutions to their underlying base native table
RESAMPLE_SOURCE_MAP = {
    "10m": "5m",
    "45m": "15m",
    "1M": "1d",
    "3M": "1d",
    "6M": "1d",
    "12M": "1d"
}

PLOT_RESOLUTION = "15m"

REST_URL = "https://api.india.delta.exchange/v2"
WS_URL = "wss://public-socket.india.delta.exchange"
PORT = 5001

# Set your custom start date here (YYYY-MM-DD or DD-MM-YYYY)
START_DATE = "2026-11-01"

# Precomputed fast lookup structures shared across modules
SYMBOL_LIST = list(SYMBOLS) if isinstance(SYMBOLS, (list, tuple)) else [SYMBOLS]
SYMBOL_SET = set(SYMBOL_LIST)
RESOLUTION_SET = set(RESOLUTIONS)
UI_RESOLUTION_SET = set(UI_RESOLUTIONS)

DERIVED_FROM_BASE = {}
for _custom_r, _base_r in RESAMPLE_SOURCE_MAP.items():
    DERIVED_FROM_BASE.setdefault(_base_r, []).append(_custom_r)

_RES_REGEX = re.compile(r"^(\d+)([smhdw])$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def get_resolution_seconds(res_str: str) -> int:
    """Fast resolution string to seconds converter."""
    s = str(res_str).strip()
    if s.endswith("M"):
        return int(s[:-1]) * 30 * 86400
    m = _RES_REGEX.match(s.lower())
    if m:
        return int(m.group(1)) * _UNIT_SECONDS[m.group(2)]
    return 900


RES_SEC_MAP = {res: get_resolution_seconds(res) for res in UI_RESOLUTIONS}
