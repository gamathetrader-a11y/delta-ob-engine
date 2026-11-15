"""Ultra-Fast Parallel Multi-Symbol & Multi-Resolution History Loader for Delta Exchange"""

import os
import sys
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
db_dir = os.path.join(parent_dir, "db")
indicator_dir = os.path.join(parent_dir, "indicator")

if db_dir not in sys.path:
    sys.path.insert(0, db_dir)
if indicator_dir not in sys.path:
    sys.path.insert(0, indicator_dir)

from config import SYMBOLS, RESOLUTIONS, REST_URL, START_DATE
from database import save_candles_bulk, get_connection, get_table_name, get_db_path
from indicator_loader import build_all_ind_dbs

SYMBOL_LIST = SYMBOLS if isinstance(SYMBOLS, (list, tuple)) else [SYMBOLS]
MAX_CANDLES_PER_REQUEST = 2000
MAX_WORKERS_PER_SYMBOL = 8


def get_resolution_seconds(res_str):
    s = str(res_str).strip()
    if s.endswith("M"):
        return int(s[:-1]) * 30 * 86400
    s = s.lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60
    elif s.endswith("h"):
        return int(s[:-1]) * 3600
    elif s.endswith("d"):
        return int(s[:-1]) * 86400
    elif s.endswith("w"):
        return int(s[:-1]) * 604800
    elif s.endswith("s"):
        return int(s[:-1])
    return 900


RES_SEC_MAP = {res: get_resolution_seconds(res) for res in RESOLUTIONS}


def create_fast_session():
    session = requests.Session()
    retry = Retry(
        total=2,
        backoff_factor=0.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(pool_connections=24, pool_maxsize=24, max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def parse_config_start_date(date_str):
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return int(datetime.strptime(date_str.strip(), fmt).timestamp())
        except ValueError:
            continue
    raise ValueError(f"Unsupported START_DATE format in config.py: '{date_str}'. Use YYYY-MM-DD or DD-MM-YYYY.")


def get_all_db_boundaries(symbol):
    conn = get_connection(symbol)
    cursor = conn.cursor()
    boundaries = {}
    with conn:
        for res in RESOLUTIONS:
            table_name = get_table_name(symbol, res)
            cursor.execute(f"""
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
            cursor.execute(f"SELECT MIN(time), MAX(time) FROM {table_name}")
            row = cursor.fetchone()
            if row and row[0] is not None:
                boundaries[res] = (int(row[0]), int(row[1]))
            else:
                boundaries[res] = (None, None)
    conn.close()
    return boundaries


def build_time_windows(start_target, end_target, resolution_seconds):
    windows = []
    step = MAX_CANDLES_PER_REQUEST * resolution_seconds
    curr_end = end_target
    while curr_end > start_target:
        curr_start = max(start_target, curr_end - step + resolution_seconds)
        windows.append((curr_start, curr_end))
        curr_end = curr_start - resolution_seconds
    return windows


def fetch_single_window(session, symbol, resolution, w_start, w_end):
    url = f"{REST_URL}/history/candles?symbol={symbol}&resolution={resolution}&start={w_start}&end={w_end}"
    try:
        res = session.get(url, timeout=6)
        if res.status_code == 200:
            data = res.json()
            if data and "result" in data and data["result"]:
                return resolution, data["result"]
    except Exception as e:
        print(f" [!] Retry failed [{symbol} | {resolution} | {w_start}->{w_end}]: {e}")
    return resolution, []


def sync_single_symbol(sym, start_target):
    session = create_fast_session()
    db_file = os.path.basename(get_db_path(sym))
    now_ts = int(time.time())

    boundaries = get_all_db_boundaries(sym)
    tasks = []

    for res in RESOLUTIONS:
        resolution_seconds = RES_SEC_MAP.get(res, get_resolution_seconds(res))
        oldest_db_ts, newest_db_ts = boundaries[res]

        if oldest_db_ts is None:
            for w_start, w_end in build_time_windows(start_target, now_ts, resolution_seconds):
                tasks.append((res, w_start, w_end))
        else:
            if oldest_db_ts > start_target + resolution_seconds:
                for w_start, w_end in build_time_windows(start_target, oldest_db_ts - resolution_seconds, resolution_seconds):
                    tasks.append((res, w_start, w_end))
            if now_ts - newest_db_ts >= resolution_seconds:
                for w_start, w_end in build_time_windows(newest_db_ts - resolution_seconds, now_ts, resolution_seconds):
                    tasks.append((res, w_start, w_end))

    if not tasks:
        session.close()
        print(f"=== {sym} ({db_file}) already up to date ===")
        return 0

    print(f"[{db_file}] Downloading {len(tasks)} API chunks across {len(RESOLUTIONS)} resolutions in parallel...")
    candles_by_res = {res: [] for res in RESOLUTIONS}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_SYMBOL) as pool:
        futures = [
            pool.submit(fetch_single_window, session, sym, res, w_start, w_end)
            for res, w_start, w_end in tasks
        ]
        for f in as_completed(futures):
            res, candles = f.result()
            if candles:
                candles_by_res[res].extend(candles)

    session.close()

    symbol_total = 0
    for res, candles in candles_by_res.items():
        if candles:
            save_candles_bulk(candles, sym, res)
            symbol_total += len(candles)

    print(f"=== Finished {sym} ({db_file}) | Total New Candles Saved: {symbol_total:,} ===")
    return symbol_total


def sync_history_from_config():
    t0 = time.time()
    print("=== Delta Exchange Ultra-Fast Parallel History Loader ===")
    print(f"Symbols: {', '.join(SYMBOL_LIST)} | Native Resolutions: {', '.join(RESOLUTIONS)} | Start Date: {START_DATE}\n")

    start_target = parse_config_start_date(START_DATE)
    grand_total = 0

    with ThreadPoolExecutor(max_workers=min(len(SYMBOL_LIST), 4)) as executor:
        futures = {executor.submit(sync_single_symbol, sym, start_target): sym for sym in SYMBOL_LIST}
        for future in as_completed(futures):
            grand_total += future.result()

    elapsed = time.time() - t0
    print(f"\n=== All Symbol Price DBs Synced in {elapsed:.1f}s! Grand Total Candles Saved: {grand_total:,} ===")

    build_all_ind_dbs()


if __name__ == "__main__":
    sync_history_from_config()
