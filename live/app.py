"""Main Flask Application for Delta Exchange Live Chart (Clean Non-Blocking Engine + Gzip + Multi-Client SSE)"""

import os
import sys
import gzip
import socket

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
db_dir = os.path.join(parent_dir, "db")
indicator_dir = os.path.join(parent_dir, "indicator")

if db_dir not in sys.path:
    sys.path.insert(0, db_dir)
if indicator_dir not in sys.path:
    sys.path.insert(0, indicator_dir)

import threading
import time
import json
import asyncio
import websockets
import requests
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, Response, request
from queue import Queue, Empty
from indicator_loader import build_all_ind_dbs, save_ind_live_timestamp, get_ob_chart_data

from config import (
    SYMBOLS, DEFAULT_SYMBOL, RESOLUTIONS, UI_RESOLUTIONS,
    RESAMPLE_SOURCE_MAP, PLOT_RESOLUTION, REST_URL, WS_URL, PORT
)
from database import (
    init_db, get_all_candles, save_candle, save_candles_bulk,
    get_table_name, get_connection, get_latest_resampled_candle
)

app = Flask(__name__)

SYMBOL_LIST = SYMBOLS if isinstance(SYMBOLS, (list, tuple)) else [SYMBOLS]
SYMBOL_SET = set(SYMBOL_LIST)
RESOLUTION_SET = set(RESOLUTIONS)
UI_RESOLUTION_SET = set(UI_RESOLUTIONS)

broadcast_queues = {(sym, res): Queue(maxsize=5) for sym in SYMBOL_LIST for res in UI_RESOLUTIONS}
_stream_subscribers = {(sym, res): set() for sym in SYMBOL_LIST for res in UI_RESOLUTIONS}
_sub_lock = threading.Lock()

db_lock = threading.Lock()
db_event = threading.Event()
pending_new_bars = []
latest_intra_Updates = {}
last_seen_bar_ts = {}

_ind_pending = set()
_ind_lock = threading.Lock()
ind_executor = ThreadPoolExecutor(max_workers=2)

DERIVED_FROM_BASE = {}
for custom_r, base_r in RESAMPLE_SOURCE_MAP.items():
    DERIVED_FROM_BASE.setdefault(base_r, []).append(custom_r)


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


RES_SEC_MAP = {res: get_resolution_seconds(res) for res in UI_RESOLUTIONS}


def norm_ts(val):
    t = int(float(val))
    if t >= 100_000_000_000_000:
        return t // 1_000_000
    elif t >= 100_000_000_000:
        return t // 1_000
    return t


def bucket_start(timestamp, resolution_sec):
    ts = norm_ts(timestamp)
    return ts - (ts % resolution_sec)


def pick_price(payload, *keys, default=0.0):
    for k in keys:
        val = payload.get(k)
        if val is not None and val != "":
            try:
                f_val = float(val)
                if f_val > 0:
                    return f_val
            except (ValueError, TypeError):
                continue
    return float(default)


def push_to_ui_queue(q, item):
    try:
        while q.qsize() >= 2:
            q.get_nowait()
    except Empty:
        pass
    try:
        q.put_nowait(item)
    except Exception:
        pass


def broadcast_to_ui(sym, res, candle):
    key = (sym, res)
    if key in broadcast_queues:
        push_to_ui_queue(broadcast_queues[key], candle)
    with _sub_lock:
        subs = tuple(_stream_subscribers.get(key, ()))
    for q in subs:
        push_to_ui_queue(q, candle)


def has_active_subscribers(sym, res) -> bool:
    with _sub_lock:
        return bool(_stream_subscribers.get((sym, res)))


def _schedule_ind_update(ts, sym, res):
    key = (sym, res)
    with _ind_lock:
        if key in _ind_pending:
            return
        _ind_pending.add(key)

    def _task():
        try:
            save_ind_live_timestamp(ts, sym, res)
        finally:
            with _ind_lock:
                _ind_pending.discard(key)

    ind_executor.submit(_task)


def enqueue_db_update(candle, sym, res):
    key = (sym, res)
    c_time = int(candle["time"])
    with db_lock:
        prev_ts = last_seen_bar_ts.get(key, 0)
        if c_time < prev_ts:
            return False
        if c_time > prev_ts:
            last_seen_bar_ts[key] = c_time
            pending_new_bars.append((candle, sym, res, True))
        else:
            latest_intra_Updates[key] = candle
    db_event.set()
    return True


def db_writer_worker():
    while True:
        db_event.wait()
        while True:
            with db_lock:
                if pending_new_bars:
                    batch = pending_new_bars[:]
                    pending_new_bars.clear()
                elif latest_intra_Updates:
                    batch = [(c, s, r, False) for (s, r), c in latest_intra_Updates.items()]
                    latest_intra_Updates.clear()
                else:
                    db_event.clear()
                    break

            for candle, sym, res, is_new_bar in batch:
                try:
                    save_candle(candle, sym, res)
                    for custom_res in DERIVED_FROM_BASE.get(res, []):
                        if is_new_bar or has_active_subscribers(sym, custom_res):
                            resampled_live = get_latest_resampled_candle(sym, custom_res, candle)
                            broadcast_to_ui(sym, custom_res, resampled_live)

                    if is_new_bar:
                        _schedule_ind_update(candle["time"], sym, res)
                except Exception as e:
                    print(f"[DB Worker Error | {sym} {res}]: {e}")


def sync_symbol_startup(sym):
    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_connections=8, pool_maxsize=8))
    current_time = int(time.time())

    latest_times = {}
    conn = get_connection(sym)
    try:
        cursor = conn.cursor()
        for res in RESOLUTIONS:
            init_db(sym, res)
            table_name = get_table_name(sym, res)
            cursor.execute(f"SELECT MAX(time) FROM {table_name}")
            row = cursor.fetchone()
            latest_times[res] = row[0] if row and row[0] else None
    finally:
        conn.close()

    for res in RESOLUTIONS:
        latest_db_time = latest_times.get(res)
        resolution_seconds = RES_SEC_MAP.get(res, get_resolution_seconds(res))

        if not latest_db_time:
            lookback = max(86400, resolution_seconds * 200)
            start_target = current_time - lookback
        else:
            start_target = latest_db_time - (resolution_seconds * 200)

        current_end = current_time
        while current_end > start_target:
            chunk_size = 2000 * resolution_seconds
            current_start = max(start_target, current_end - chunk_size)

            url = f"{REST_URL}/history/candles?symbol={sym}&resolution={res}&start={current_start}&end={current_end}"
            try:
                res_api = session.get(url, timeout=8)
                data = res_api.json()
            except Exception as e:
                print(f"[{sym}.db | {res}] API Request Error: {e}")
                break

            if data and "result" in data and len(data["result"]) > 0:
                candles = data["result"]
                save_candles_bulk(candles, sym, res)

                oldest_ts_in_batch = min(norm_ts(c["time"]) for c in candles)
                if oldest_ts_in_batch >= current_end:
                    current_end = current_start - 1
                else:
                    current_end = oldest_ts_in_batch - 1
            else:
                current_end = current_start - 1

    session.close()
    print(f"[{sym}.db] Fast startup sync complete.")


def check_and_fill_gaps_for_all():
    t0 = time.time()
    print("Running high-speed parallel startup sync across all symbols...")
    with ThreadPoolExecutor(max_workers=min(len(SYMBOL_LIST), 6)) as executor:
        list(executor.map(sync_symbol_startup, SYMBOL_LIST))
    print(f"All symbols synced in {time.time() - t0:.1f}s!")
    build_all_ind_dbs()


async def live_websocket_worker():
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                family=socket.AF_INET,
                open_timeout=30,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
                max_size=2**22
            ) as ws:
                print(f"Connected to Delta Exchange WebSocket ({', '.join(SYMBOL_LIST)} | {', '.join(RESOLUTIONS)})")

                channels = [{"name": f"candlestick_{res}", "symbols": SYMBOL_LIST} for res in RESOLUTIONS]
                subscribe_msg = {"type": "subscribe", "payload": {"channels": channels}}
                await ws.send(json.dumps(subscribe_msg))

                async for message in ws:
                    payload = json.loads(message)
                    channel_type = str(payload.get("type", ""))

                    if channel_type.startswith("candlestick_"):
                        res = channel_type[12:]
                        sym = str(payload.get("symbol") or payload.get("s") or payload.get("sy") or "").upper()

                        if sym in SYMBOL_SET and res in RESOLUTION_SET:
                            close_val = pick_price(payload, "close", "c")
                            if close_val <= 0:
                                continue

                            open_val = pick_price(payload, "open", "o", default=close_val)
                            high_val = max(pick_price(payload, "high", "h", default=close_val), open_val, close_val)
                            low_val = min(pick_price(payload, "low", "l", default=close_val), open_val, close_val)
                            vol_val = float(payload.get("volume") or payload.get("v") or 0.0)

                            resolution_seconds = RES_SEC_MAP.get(res, 60)
                            cst = payload.get("candle_start_time") or payload.get("cst")
                            if cst is not None:
                                aligned_ts = norm_ts(cst)
                            else:
                                raw_ts = payload.get("time") or payload.get("timestamp") or int(time.time())
                                aligned_ts = bucket_start(raw_ts, resolution_seconds)

                            candle = {
                                "time": aligned_ts,
                                "open": open_val,
                                "high": high_val,
                                "low": low_val,
                                "close": close_val,
                                "volume": vol_val
                            }

                            if enqueue_db_update(candle, sym, res):
                                broadcast_to_ui(sym, res, candle)

        except Exception as e:
            print(f"WebSocket Error: {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)


def start_ws_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(live_websocket_worker())


def _fast_json_response(data_obj):
    raw_bytes = json.dumps(data_obj, separators=(",", ":")).encode("utf-8")
    accept_enc = request.headers.get("Accept-Encoding", "")
    if len(raw_bytes) > 1024 and "gzip" in accept_enc.lower():
        compressed = gzip.compress(raw_bytes, compresslevel=4)
        resp = Response(compressed, mimetype="application/json")
        resp.headers["Content-Encoding"] = "gzip"
        resp.headers["Vary"] = "Accept-Encoding"
        resp.headers["Content-Length"] = str(len(compressed))
        return resp
    return Response(raw_bytes, mimetype="application/json")


@app.route("/")
def index():
    default_sym = DEFAULT_SYMBOL if DEFAULT_SYMBOL in SYMBOL_SET else SYMBOL_LIST[0]
    default_res = PLOT_RESOLUTION if PLOT_RESOLUTION in UI_RESOLUTION_SET else UI_RESOLUTIONS[0]
    return render_template(
        "index.html",
        symbol=default_sym,
        symbols=SYMBOL_LIST,
        resolution=default_res,
        resolutions=UI_RESOLUTIONS
    )


@app.route("/api/history")
def api_history():
    sym = request.args.get("symbol", DEFAULT_SYMBOL)
    res = request.args.get("resolution", PLOT_RESOLUTION)
    if sym not in SYMBOL_SET:
        sym = SYMBOL_LIST[0]
    if res not in UI_RESOLUTION_SET:
        res = UI_RESOLUTIONS[0]
    return _fast_json_response(get_all_candles(sym, res))


@app.route("/api/ob_data")
def api_ob_data():
    sym = request.args.get("symbol", DEFAULT_SYMBOL)
    res = request.args.get("resolution", PLOT_RESOLUTION)
    if sym not in SYMBOL_SET:
        sym = SYMBOL_LIST[0]
    if res not in UI_RESOLUTION_SET:
        res = UI_RESOLUTIONS[0]
    return _fast_json_response(get_ob_chart_data(sym, res))


@app.route("/api/stream")
def api_stream():
    sym = request.args.get("symbol", DEFAULT_SYMBOL)
    res = request.args.get("resolution", PLOT_RESOLUTION)
    if sym not in SYMBOL_SET:
        sym = SYMBOL_LIST[0]
    if res not in UI_RESOLUTION_SET:
        res = UI_RESOLUTIONS[0]

    def event_stream():
        client_q = Queue(maxsize=5)
        key = (sym, res)
        with _sub_lock:
            _stream_subscribers[key].add(client_q)
        try:
            while True:
                try:
                    candle = client_q.get(timeout=15)
                    yield f"data: {json.dumps(candle, separators=(',', ':'))}\n\n"
                except Empty:
                    yield ": keepalive\n\n"
        finally:
            with _sub_lock:
                _stream_subscribers[key].discard(client_q)

    resp = Response(event_stream(), mimetype="text/event-stream")
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    return resp


def free_port_pure_python(port):
    if os.name == "nt":
        return
    import signal
    import subprocess
    import time

    my_pid = os.getpid()
    try:
        for pid_dir in os.listdir("/proc"):
            if not pid_dir.isdigit():
                continue
            pid = int(pid_dir)
            if pid == my_pid:
                continue
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    cmd = f.read().decode("utf-8", errors="ignore").replace("\x00", " ")
                if "live/app.py" in cmd or "gunicorn" in cmd:
                    print(f"[Port Cleanup] Killing old zombie process PID {pid}: {cmd.strip()}")
                    os.kill(pid, signal.SIGKILL)
            except Exception:
                pass
    except Exception:
        pass

    for cmd in (f"fuser -k {port}/tcp", f"lsof -ti:{port} | xargs -r kill -9"):
        try:
            subprocess.run(cmd, shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        except Exception:
            pass

    time.sleep(1.0)


if __name__ == "__main__":
    run_port = int(os.environ.get("PORT", PORT))
    free_port_pure_python(run_port)

    db_thread = threading.Thread(target=db_writer_worker, daemon=True)
    db_thread.start()

    startup_thread = threading.Thread(target=check_and_fill_gaps_for_all, daemon=True)
    startup_thread.start()

    ws_thread = threading.Thread(target=start_ws_thread, daemon=True)
    ws_thread.start()

    print(f"Starting Multi-Symbol & Multi-Timeframe Flask server on http://0.0.0.0:{run_port}")
    app.run(host="0.0.0.0", port=run_port, debug=False, use_reloader=False, threaded=True)
