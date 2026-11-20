"""Standalone Telegram Alert Tracker & Sender for OB Setup & OB Engine"""

import os
import time
import sqlite3
import threading
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import requests
from requests.adapters import HTTPAdapter

INDICATOR_DIR = Path(__file__).resolve().parent
R2_DIR = INDICATOR_DIR.parent
IND_DB_DIR = R2_DIR / "db" / "indicator_db"
ALERT_DB_PATH = IND_DB_DIR / "IND_ALERTS.db"

KOLKATA_TZ = ZoneInfo("Asia/Kolkata")
_ALERT_LOCK = threading.Lock()
_WELCOME_SENT = False
_ALERT_DB_INIT = False

_CFG_CACHE = None
_CFG_MTIME = None
_CFG_PATH = None

_HTTP_SESSION = requests.Session()
_HTTP_SESSION.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=4))


def _resolve_env_file() -> Path:
    for folder in (INDICATOR_DIR, R2_DIR):
        for name in ("telegram.env", "telegram.txt", "telegram.env.txt"):
            candidate = folder / name
            if candidate.is_file():
                return candidate
    return INDICATOR_DIR / "telegram.env"


def _parse_set_field(val: str, upper: bool = False) -> set[str]:
    cleaned = val.strip("[]()")
    items = []
    for item in cleaned.split(","):
        s = item.strip().strip('"').strip("'")
        if s:
            if upper:
                s = s.replace(".", "_").replace("-", "_").upper()
            items.append(s)
    return set(items)


def load_telegram_env(env_path: Path | None = None) -> dict:
    global _CFG_CACHE, _CFG_MTIME, _CFG_PATH

    if env_path is None:
        env_path = _CFG_PATH if (_CFG_PATH is not None and _CFG_PATH.is_file()) else _resolve_env_file()
        _CFG_PATH = env_path

    try:
        mtime = env_path.stat().st_mtime_ns if env_path.is_file() else -1
    except OSError:
        mtime = -1

    if _CFG_CACHE is not None and mtime == _CFG_MTIME:
        return _CFG_CACHE

    cfg = {
        "TELEGRAM_ENABLED": True,
        "TELEGRAM_BOT_TOKEN": "",
        "TELEGRAM_CHAT_ID": "",
        "TELEGRAM_ALERT_SYMBOLS": set(),
        "TELEGRAM_ALERT_RESOLUTIONS": {"15m", "30m", "45m"},
        "TELEGRAM_STARTUP_LOOKBACK_HOURS": 24.0,
    }

    if mtime != -1:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if not line or "=" not in line:
                continue

            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")

            if key == "TELEGRAM_ENABLED":
                cfg[key] = val.lower() in ("1", "true", "yes", "on")
            elif key == "TELEGRAM_ALERT_SYMBOLS":
                cfg[key] = _parse_set_field(val, upper=True)
            elif key == "TELEGRAM_ALERT_RESOLUTIONS":
                cfg[key] = _parse_set_field(val, upper=False)
            elif key == "TELEGRAM_STARTUP_LOOKBACK_HOURS":
                try:
                    cfg[key] = float(val)
                except ValueError:
                    cfg[key] = 24.0
            elif key in cfg:
                cfg[key] = val

    if os.environ.get("TELEGRAM_ENABLED") is not None:
        val = os.environ["TELEGRAM_ENABLED"].split("#", 1)[0].strip().strip('"').strip("'")
        if val:
            cfg["TELEGRAM_ENABLED"] = val.lower() in ("1", "true", "yes", "on")

    if os.environ.get("TELEGRAM_BOT_TOKEN"):
        cfg["TELEGRAM_BOT_TOKEN"] = os.environ["TELEGRAM_BOT_TOKEN"].split("#", 1)[0].strip().strip('"').strip("'")

    if os.environ.get("TELEGRAM_CHAT_ID"):
        cfg["TELEGRAM_CHAT_ID"] = os.environ["TELEGRAM_CHAT_ID"].split("#", 1)[0].strip().strip('"').strip("'")

    if os.environ.get("TELEGRAM_ALERT_SYMBOLS") is not None:
        val = os.environ["TELEGRAM_ALERT_SYMBOLS"].split("#", 1)[0].strip()
        if val:
            cfg["TELEGRAM_ALERT_SYMBOLS"] = _parse_set_field(val, upper=True)

    if os.environ.get("TELEGRAM_ALERT_RESOLUTIONS") is not None:
        val = os.environ["TELEGRAM_ALERT_RESOLUTIONS"].split("#", 1)[0].strip()
        if val:
            cfg["TELEGRAM_ALERT_RESOLUTIONS"] = _parse_set_field(val, upper=False)

    if os.environ.get("TELEGRAM_STARTUP_LOOKBACK_HOURS") is not None:
        val = os.environ["TELEGRAM_STARTUP_LOOKBACK_HOURS"].split("#", 1)[0].strip().strip('"').strip("'")
        if val:
            try:
                cfg["TELEGRAM_STARTUP_LOOKBACK_HOURS"] = float(val)
            except ValueError:
                pass

    _CFG_MTIME = mtime
    _CFG_CACHE = cfg
    return cfg


def _get_alert_conn() -> sqlite3.Connection:
    global _ALERT_DB_INIT
    IND_DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(ALERT_DB_PATH, timeout=30)
    if not _ALERT_DB_INIT:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sent_ob_alerts (
                symbol TEXT,
                resolution TEXT,
                candidate_ts INTEGER,
                breakout_ts INTEGER,
                sent_at INTEGER,
                PRIMARY KEY (symbol, resolution, candidate_ts)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sent_simple_ob_alerts (
                symbol TEXT,
                resolution TEXT,
                candidate_ts INTEGER,
                sent_at INTEGER,
                PRIMARY KEY (symbol, resolution, candidate_ts)
            )
        """)
        _ALERT_DB_INIT = True
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def send_telegram_message(text: str) -> bool:
    cfg = load_telegram_env()
    if not cfg["TELEGRAM_ENABLED"]:
        return False

    token = str(cfg["TELEGRAM_BOT_TOKEN"]).strip()
    if token.lower().startswith("bot"):
        token = token[3:].strip()

    chat_id = str(cfg["TELEGRAM_CHAT_ID"]).strip()
    if (
        not token
        or "YOUR_BOT_TOKEN" in token
        or token.lower().startswith("xxx")
        or not chat_id
        or "YOUR_CHAT_ID" in chat_id
        or chat_id.lower().startswith("xxx")
    ):
        print("[Telegram Warning] Valid Bot Token or Chat ID not set in telegram.env or Cloud Environment.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = _HTTP_SESSION.post(url, json=payload, timeout=8)
        if resp.status_code == 200:
            return True
        print(f"[Telegram Error] HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[Telegram Send Exception]: {e}")
    return False


def send_startup_welcome_message(symbols: list[str]) -> bool:
    global _WELCOME_SENT
    with _ALERT_LOCK:
        if _WELCOME_SENT:
            return True
        _WELCOME_SENT = True

    cfg = load_telegram_env()
    allowed_syms = cfg["TELEGRAM_ALERT_SYMBOLS"]
    active_syms = sorted(allowed_syms) if allowed_syms else symbols
    res_list = sorted(cfg["TELEGRAM_ALERT_RESOLUTIONS"])
    lookback_h = int(cfg["TELEGRAM_STARTUP_LOOKBACK_HOURS"])
    now_ist = datetime.now(tz=KOLKATA_TZ).strftime("%Y-%m-%d %H:%M:%S IST")

    msg = (
        f"✅ <b>DELTA EXCHANGE OB &amp; OB SETUP ENGINE CONNECTED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕒 <b>Time:</b> <code>{now_ist}</code>\n"
        f"📊 <b>Alert Symbols:</b> <code>{', '.join(active_syms)}</code>\n"
        f"⏱ <b>Alert TFs:</b> <code>{', '.join(res_list)}</code>\n"
        f"📡 <b>Mode:</b> <code>{lookback_h}h Backfill + Live Streaming Active</code>"
    )
    ok = send_telegram_message(msg)
    if ok:
        print("[Telegram] Welcome connection message sent successfully!")
    return ok


def _fmt_num(val) -> str:
    if val is None or pd.isna(val):
        return "N/A"
    f = float(val)
    return f"{f:,.2f}" if abs(f) >= 10 else f"{f:,.4f}"


def _format_5c_str(row: pd.Series | dict) -> str:
    confs = []
    for c_res in ("1m", "3m", "5m"):
        val = row.get(f"five_candle_movement_{c_res}")
        if val is not None and pd.notna(val):
            confs.append(f"{c_res} ({float(val):+.2f})")
    return ", ".join(confs) if confs else "Confirmed"


def format_ob_telegram_alert(row: pd.Series | dict, is_recent_24h: bool = False) -> str:
    is_bull = int(row["setup_trend"]) == 1
    tag = "⏳ <i>[Last 24h OB Setup]</i>\n" if is_recent_24h else "⚡ <i>[Live OB Setup]</i>\n"
    direction_str = "🟢 <b>BULLISH OB SETUP (UPTREND)</b>" if is_bull else "🔴 <b>BEARISH OB SETUP (DOWNTREND)</b>"

    sym = row["symbol"]
    res = row["resolution"]
    ob_time = row["datetime_kolkata"]

    bb_val = row.get("breakout_breakdown_candle")
    bb_ts = int(bb_val) if (bb_val is not None and pd.notna(bb_val)) else 0
    bb_time = (
        datetime.fromtimestamp(bb_ts, tz=KOLKATA_TZ).strftime("%Y-%m-%d %H:%M:%S")
        if bb_ts > 0 else "N/A"
    )

    return (
        f"{tag}{direction_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Symbol:</b> <code>{sym}</code> | <b>TF:</b> <code>{res}</code>\n"
        f"🕒 <b>OB Setup Candle (IST):</b> <code>{ob_time}</code>\n"
        f"⚡ <b>BO/BD Confirmed:</b> <code>{bb_time}</code>\n"
        f"📊 <b>OB Setup Zone:</b> <code>{_fmt_num(row.get('low'))}</code> – <code>{_fmt_num(row.get('high'))}</code>\n"
        f"🎯 <b>Price Edge:</b> <code>{_fmt_num(row.get('price_edge'))}</code>\n"
        f"📏 <b>Setup Move:</b> <code>{_fmt_num(row.get('setup_movement'))}</code>\n"
        f"🔥 <b>5C Streak:</b> <code>{_format_5c_str(row)}</code>"
    )


def format_simple_ob_telegram_alert(row: dict, symbol: str, resolution: str, is_recent_24h: bool = False) -> str:
    is_bull = int(row["setup_trend"]) == 1
    tag = "⏳ <i>[Last 24h OB]</i>\n" if is_recent_24h else "⚡ <i>[Live OB Created]</i>\n"
    direction_str = "🟢 <b>BULLISH OB (UPTREND)</b>" if is_bull else "🔴 <b>BEARISH OB (DOWNTREND)</b>"

    c_ts = int(row["time"])
    ob_time = row.get("datetime_kolkata") or datetime.fromtimestamp(c_ts, tz=KOLKATA_TZ).strftime("%Y-%m-%d %H:%M:%S")

    return (
        f"{tag}{direction_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Symbol:</b> <code>{symbol}</code> | <b>TF:</b> <code>{resolution}</code>\n"
        f"🕒 <b>OB Candle (IST):</b> <code>{ob_time}</code>\n"
        f"📊 <b>OB Zone:</b> <code>{_fmt_num(row.get('low'))}</code> – <code>{_fmt_num(row.get('high'))}</code>\n"
        f"📏 <b>Momentum:</b> <code>{_fmt_num(row.get('momentum'))}</code>\n"
        f"🔥 <b>5C Streak:</b> <code>{_format_5c_str(row)}</code>"
    )


def process_ob_alerts(confirmed_input, symbol: str, resolution: str, send_live: bool = False) -> None:
    if isinstance(confirmed_input, list):
        if not confirmed_input:
            return
        records = confirmed_input
    elif isinstance(confirmed_input, pd.DataFrame):
        if confirmed_input.empty or "candidate" not in confirmed_input.columns:
            return
        confirmed = confirmed_input[confirmed_input["candidate"] == 1]
        if confirmed.empty:
            return
        records = confirmed.to_dict("records")
    else:
        return

    cfg = load_telegram_env()
    clean_sym = str(symbol).replace(".", "_").replace("-", "_").upper()

    allowed_syms = cfg["TELEGRAM_ALERT_SYMBOLS"]
    allowed_res = cfg["TELEGRAM_ALERT_RESOLUTIONS"]
    should_notify = ((not allowed_syms) or (clean_sym in allowed_syms)) and ((not allowed_res) or (resolution in allowed_res))

    lookback_seconds = int(cfg["TELEGRAM_STARTUP_LOOKBACK_HOURS"] * 3600)
    now_ts = int(time.time())
    cutoff_ts = now_ts - lookback_seconds
    new_alerts_to_send = []

    with _ALERT_LOCK:
        conn = _get_alert_conn()
        try:
            existing_rows = conn.execute(
                "SELECT candidate_ts FROM sent_ob_alerts WHERE symbol = ? AND resolution = ?",
                (clean_sym, resolution),
            ).fetchall()
            seen_ts = {int(r[0]) for r in existing_rows}

            to_insert = []
            for row in records:
                c_ts = int(row["timestamp"])
                if c_ts not in seen_ts:
                    bb_val = row.get("breakout_breakdown_candle")
                    bb_ts = int(bb_val) if (bb_val is not None and pd.notna(bb_val)) else 0
                    to_insert.append((clean_sym, resolution, c_ts, bb_ts, now_ts))

                    event_ts = max(c_ts, bb_ts)
                    if should_notify:
                        if send_live:
                            new_alerts_to_send.append(format_ob_telegram_alert(row, is_recent_24h=False))
                        elif event_ts >= cutoff_ts:
                            new_alerts_to_send.append(format_ob_telegram_alert(row, is_recent_24h=True))

            if to_insert:
                with conn:
                    conn.executemany(
                        "INSERT OR IGNORE INTO sent_ob_alerts (symbol, resolution, candidate_ts, breakout_ts, sent_at) VALUES (?, ?, ?, ?, ?)",
                        to_insert,
                    )
        finally:
            conn.close()

    for msg in new_alerts_to_send:
        if send_telegram_message(msg):
            print(f"[Telegram OB Setup Alert Sent] {clean_sym} | {resolution}")
            time.sleep(0.25)


def process_simple_ob_alerts(simple_obs: list[dict], symbol: str, resolution: str, send_live: bool = False) -> None:
    if not simple_obs:
        return

    cfg = load_telegram_env()
    clean_sym = str(symbol).replace(".", "_").replace("-", "_").upper()

    allowed_syms = cfg["TELEGRAM_ALERT_SYMBOLS"]
    allowed_res = cfg["TELEGRAM_ALERT_RESOLUTIONS"]
    should_notify = ((not allowed_syms) or (clean_sym in allowed_syms)) and ((not allowed_res) or (resolution in allowed_res))

    lookback_seconds = int(cfg["TELEGRAM_STARTUP_LOOKBACK_HOURS"] * 3600)
    now_ts = int(time.time())
    cutoff_ts = now_ts - lookback_seconds
    new_alerts_to_send = []

    with _ALERT_LOCK:
        conn = _get_alert_conn()
        try:
            existing_rows = conn.execute(
                "SELECT candidate_ts FROM sent_simple_ob_alerts WHERE symbol = ? AND resolution = ?",
                (clean_sym, resolution),
            ).fetchall()
            seen_ts = {int(r[0]) for r in existing_rows}

            to_insert = []
            for row in simple_obs:
                c_ts = int(row["time"])
                if c_ts not in seen_ts:
                    to_insert.append((clean_sym, resolution, c_ts, now_ts))
                    if should_notify:
                        if send_live:
                            new_alerts_to_send.append(format_simple_ob_telegram_alert(row, clean_sym, resolution, is_recent_24h=False))
                        elif c_ts >= cutoff_ts:
                            new_alerts_to_send.append(format_simple_ob_telegram_alert(row, clean_sym, resolution, is_recent_24h=True))

            if to_insert:
                with conn:
                    conn.executemany(
                        "INSERT OR IGNORE INTO sent_simple_ob_alerts (symbol, resolution, candidate_ts, sent_at) VALUES (?, ?, ?, ?)",
                        to_insert,
                    )
        finally:
            conn.close()

    for msg in new_alerts_to_send:
        if send_telegram_message(msg):
            print(f"[Telegram OB Alert Sent] {clean_sym} | {resolution}")
            time.sleep(0.25)