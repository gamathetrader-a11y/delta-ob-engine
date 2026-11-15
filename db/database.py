"""SQLite Database Handler for Delta Exchange Chart (WAL Mode + Vectorized NumPy Resampling)"""

import os
import sqlite3
import threading
from datetime import datetime, timezone
import numpy as np
from config import RESAMPLE_SOURCE_MAP

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_INITIALIZED_TABLES = set()
_WAL_INITIALIZED_DBS = set()
_INIT_LOCK = threading.Lock()

_CLEAN_SYM_CACHE = {}
_DB_PATH_CACHE = {}
_TABLE_NAME_CACHE = {}


def _clean_sym(symbol: str) -> str:
    s = _CLEAN_SYM_CACHE.get(symbol)
    if s is None:
        s = str(symbol).replace(".", "_").replace("-", "_").upper()
        _CLEAN_SYM_CACHE[symbol] = s
    return s


def get_db_path(symbol):
    path = _DB_PATH_CACHE.get(symbol)
    if path is None:
        os.makedirs(CURRENT_DIR, exist_ok=True)
        path = os.path.join(CURRENT_DIR, f"{_clean_sym(symbol)}.db")
        _DB_PATH_CACHE[symbol] = path
    return path


def get_connection(symbol):
    db_path = get_db_path(symbol)
    conn = sqlite3.connect(db_path, timeout=30)
    if db_path not in _WAL_INITIALIZED_DBS:
        with _INIT_LOCK:
            if db_path not in _WAL_INITIALIZED_DBS:
                conn.execute("PRAGMA journal_mode=WAL;")
                _WAL_INITIALIZED_DBS.add(db_path)
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-32000;")
    conn.execute("PRAGMA mmap_size=268435456;")
    return conn


def get_table_name(symbol, resolution):
    key = (symbol, resolution)
    t_name = _TABLE_NAME_CACHE.get(key)
    if t_name is None:
        t_name = f"candles_{_clean_sym(symbol)}_{resolution}"
        _TABLE_NAME_CACHE[key] = t_name
    return t_name


def get_custom_bucket_start(ts, target_res):
    ts = int(ts)
    if target_res == "10m":
        return ts - (ts % 600)
    elif target_res == "45m":
        return ts - (ts % 2700)
    elif str(target_res).endswith("M"):
        months_step = int(target_res[:-1])
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        bucket_month = ((dt.month - 1) // months_step) * months_step + 1
        return int(datetime(dt.year, bucket_month, 1, tzinfo=timezone.utc).timestamp())
    return ts


def _compute_bucket_times_np(times: np.ndarray, target_res: str) -> np.ndarray:
    if target_res == "10m":
        return times - (times % 600)
    if target_res == "45m":
        return times - (times % 2700)
    if str(target_res).endswith("M"):
        months_step = int(target_res[:-1])
        months_since_epoch = times.astype("datetime64[s]").astype("datetime64[M]").astype("int64")
        bucket_months = (months_since_epoch // months_step) * months_step
        return bucket_months.astype("datetime64[M]").astype("datetime64[s]").astype("int64")
    return times


def _resample_arrays(times, opens, highs, lows, closes, vols, target_res):
    n = len(times)
    b_times = _compute_bucket_times_np(times, target_res)

    if n == 1:
        idx_start = np.array([0], dtype="int64")
    else:
        change_idx = np.flatnonzero(b_times[1:] != b_times[:-1]) + 1
        idx_start = np.empty(len(change_idx) + 1, dtype="int64")
        idx_start[0] = 0
        idx_start[1:] = change_idx

    unique_b = b_times[idx_start]
    m = len(unique_b)
    idx_end = np.empty(m, dtype="int64")
    idx_end[:-1] = idx_start[1:] - 1
    idx_end[-1] = n - 1

    r_open = opens[idx_start].copy()
    r_close = closes[idx_end]
    r_high = np.maximum.reduceat(np.maximum(highs, closes), idx_start)
    r_low = np.minimum.reduceat(np.where(lows > 0, np.minimum(lows, closes), closes), idx_start)
    r_vol = np.add.reduceat(vols, idx_start)

    if m > 1:
        flat_mask = (np.abs(r_open[1:] - r_close[1:]) < 1e-8) & (np.abs(r_high[1:] - r_low[1:]) < 1e-8)
        r_open[1:] = np.where(flat_mask, r_close[:-1], r_open[1:])

    r_high = np.maximum(r_high, np.maximum(r_open, r_close))
    r_low = np.where(r_low > 0, np.minimum(r_low, np.minimum(r_open, r_close)), np.minimum(r_open, r_close))
    return unique_b, r_open, r_high, r_low, r_close, r_vol


def resample_candles(base_candles, target_res):
    if not base_candles:
        return []

    n = len(base_candles)
    times = np.fromiter((int(c["time"]) for c in base_candles), dtype="int64", count=n)
    opens = np.fromiter((float(c["open"]) for c in base_candles), dtype="float64", count=n)
    highs = np.fromiter((float(c["high"]) for c in base_candles), dtype="float64", count=n)
    lows = np.fromiter((float(c["low"]) for c in base_candles), dtype="float64", count=n)
    closes = np.fromiter((float(c["close"]) for c in base_candles), dtype="float64", count=n)
    vols = np.fromiter((float(c.get("volume", 0.0)) for c in base_candles), dtype="float64", count=n)

    unique_b, r_open, r_high, r_low, r_close, r_vol = _resample_arrays(
        times, opens, highs, lows, closes, vols, target_res
    )
    return [
        {"time": t, "open": o, "high": h, "low": l, "close": c, "volume": v}
        for t, o, h, l, c, v in zip(
            unique_b.tolist(), r_open.tolist(), r_high.tolist(),
            r_low.tolist(), r_close.tolist(), r_vol.tolist(),
        )
    ]


def init_db(symbol, resolution):
    base_res = RESAMPLE_SOURCE_MAP.get(resolution, resolution)
    table_name = get_table_name(symbol, base_res)
    if table_name in _INITIALIZED_TABLES:
        return
    with _INIT_LOCK:
        if table_name in _INITIALIZED_TABLES:
            return
        conn = get_connection(symbol)
        try:
            with conn:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        time INTEGER PRIMARY KEY,
                        symbol TEXT,
                        resolution TEXT,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL
                    )
                """)
                conn.execute(f"DELETE FROM {table_name} WHERE time > 10000000000")
            _INITIALIZED_TABLES.add(table_name)
        finally:
            conn.close()


def save_candle(candle, symbol, resolution):
    init_db(symbol, resolution)
    conn = get_connection(symbol)
    try:
        table_name = get_table_name(symbol, resolution)
        c_time = int(candle["time"])
        c_open = float(candle["open"])
        c_high = float(candle["high"])
        c_low = float(candle["low"])
        c_close = float(candle["close"])
        c_vol = float(candle.get("volume", 0.0))

        with conn:
            conn.execute(f"""
                INSERT INTO {table_name} (time, symbol, resolution, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(time) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume
            """, (c_time, symbol, resolution, c_open, c_high, c_low, c_close, c_vol))
    finally:
        conn.close()


def _norm_ts_fast(val):
    t = int(float(val))
    if t >= 100_000_000_000_000:
        return t // 1_000_000
    if t >= 100_000_000_000:
        return t // 1_000
    while t > 10_000_000_000:
        t //= 1_000
    return t


def save_candles_bulk(candles_list, symbol, resolution):
    if not candles_list:
        return

    init_db(symbol, resolution)
    conn = get_connection(symbol)
    try:
        table_name = get_table_name(symbol, resolution)
        data = [
            (
                _norm_ts_fast(c["time"]),
                symbol,
                resolution,
                float(c["open"]),
                float(c["high"]),
                float(c["low"]),
                float(c["close"]),
                float(c.get("volume", 0.0)),
            )
            for c in candles_list
        ]
        data.sort(key=lambda r: r[0])

        with conn:
            conn.executemany(f"""
                INSERT OR REPLACE INTO {table_name} (time, symbol, resolution, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, data)
    finally:
        conn.close()


def get_all_candles_arrays(symbol, resolution):
    base_res = RESAMPLE_SOURCE_MAP.get(resolution, resolution)
    init_db(symbol, base_res)
    table_name = get_table_name(symbol, base_res)

    conn = get_connection(symbol)
    try:
        rows = conn.execute(
            f"SELECT time, open, high, low, close, COALESCE(volume, 0.0) FROM {table_name} ORDER BY time ASC"
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return None

    arr = np.array(rows, dtype="float64")
    times = arr[:, 0].astype("int64")
    opens = arr[:, 1]
    highs = arr[:, 2]
    lows = arr[:, 3]
    closes = arr[:, 4]
    vols = arr[:, 5]

    if resolution in RESAMPLE_SOURCE_MAP:
        return _resample_arrays(times, opens, highs, lows, closes, vols, resolution)
    return times, opens, highs, lows, closes, vols


def get_all_candles(symbol, resolution):
    base_res = RESAMPLE_SOURCE_MAP.get(resolution, resolution)
    init_db(symbol, base_res)
    table_name = get_table_name(symbol, base_res)

    conn = get_connection(symbol)
    try:
        rows = conn.execute(
            f"SELECT time, open, high, low, close, COALESCE(volume, 0.0) FROM {table_name} ORDER BY time ASC"
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    if resolution in RESAMPLE_SOURCE_MAP:
        arr = np.array(rows, dtype="float64")
        unique_b, r_open, r_high, r_low, r_close, r_vol = _resample_arrays(
            arr[:, 0].astype("int64"), arr[:, 1], arr[:, 2], arr[:, 3], arr[:, 4], arr[:, 5], resolution
        )
        return [
            {"time": t, "open": o, "high": h, "low": l, "close": c, "volume": v}
            for t, o, h, l, c, v in zip(
                unique_b.tolist(), r_open.tolist(), r_high.tolist(),
                r_low.tolist(), r_close.tolist(), r_vol.tolist(),
            )
        ]

    return [
        {
            "time": int(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[5]),
        }
        for r in rows
    ]


def get_latest_resampled_candle(symbol, custom_res, base_candle):
    base_res = RESAMPLE_SOURCE_MAP[custom_res]
    b_start = get_custom_bucket_start(base_candle["time"], custom_res)

    conn = get_connection(symbol)
    try:
        table_name = get_table_name(symbol, base_res)
        rows = conn.execute(
            f"SELECT open, high, low, close, COALESCE(volume, 0.0) FROM {table_name} WHERE time >= ? ORDER BY time ASC",
            (b_start,),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {
            "time": b_start,
            "open": float(base_candle["open"]),
            "high": float(base_candle["high"]),
            "low": float(base_candle["low"]),
            "close": float(base_candle["close"]),
            "volume": float(base_candle.get("volume", 0.0)),
        }

    open_val = float(rows[0][0])
    high_val = max(float(r[1]) for r in rows)
    low_vals = [float(r[2]) for r in rows if float(r[2]) > 0]
    low_val = min(low_vals) if low_vals else open_val
    close_val = float(rows[-1][3])
    vol_val = sum(float(r[4]) for r in rows)

    return {
        "time": b_start,
        "open": open_val,
        "high": max(high_val, open_val, close_val),
        "low": min(low_val, open_val, close_val),
        "close": close_val,
        "volume": vol_val,
    }
