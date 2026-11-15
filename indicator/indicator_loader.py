"""Pure-RAM Ultra-Fast Vectorized Order Block (Stages 1-10) & R7 Impulse-Retrace Engine
- Computes all 10 OB stages and R7 wave turning points 100% in RAM using NumPy.
- Eliminates 51 SQLite indicator table writes per symbol (zero IND_<SYMBOL>.db disk wear).
- Keeps pre-serialized chart payloads cached in RAM for instant (<1ms) /api/ob_data responses.
"""

import re
import sys
import math
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import numpy as np

INDICATOR_DIR = Path(__file__).resolve().parent
R2_DIR = INDICATOR_DIR.parent
DB_DIR = R2_DIR / "db"

if str(DB_DIR) not in sys.path:
    sys.path.insert(0, str(DB_DIR))
if str(INDICATOR_DIR) not in sys.path:
    sys.path.insert(0, str(INDICATOR_DIR))

from config import SYMBOLS, UI_RESOLUTIONS, RESAMPLE_SOURCE_MAP
from database import get_all_candles, get_all_candles_arrays, get_db_path
from telegram_notifier import process_ob_alerts, send_startup_welcome_message

IST_OFFSET_SECONDS = 19800  # +05:30 fixed offset (Asia/Kolkata)
SYMBOL_LIST = SYMBOLS if isinstance(SYMBOLS, (list, tuple)) else [SYMBOLS]
CONFIRM_RES = ("1m", "3m", "5m")

DERIVED_FROM_BASE = {}
for custom_r, base_r in RESAMPLE_SOURCE_MAP.items():
    DERIVED_FROM_BASE.setdefault(base_r, []).append(custom_r)

_SYMBOL_LOCKS = {str(s).replace(".", "_").replace("-", "_").upper(): threading.Lock() for s in SYMBOL_LIST}
_RAM_OVERLAY_CACHE = {}


def res_to_seconds(res: str) -> int:
    s = str(res).strip()
    if s.endswith("M"):
        return int(s[:-1]) * 30 * 86400
    m = re.fullmatch(r"(\d+)([mhdw])", s.lower())
    if not m:
        return 900
    return int(m.group(1)) * {"m": 60, "h": 3600, "d": 86400, "w": 604800}[m.group(2)]


def format_kolkata_timestamps(ts_arr: np.ndarray) -> np.ndarray:
    n = len(ts_arr)
    if n == 0:
        return np.empty(0, dtype="<U19")
    s_arr = np.datetime_as_string(
        (np.asarray(ts_arr, dtype="int64") + IST_OFFSET_SECONDS).astype("datetime64[s]"),
        unit="s",
    )
    s_arr.view(np.uint32).reshape(n, -1)[:, 10] = 32
    return s_arr


def detect_r7_states_np(highs: np.ndarray, lows: np.ndarray) -> dict[int, int]:
    n = len(highs)
    if n == 0:
        return {}

    low = [float(lows[0])]
    high = [float(highs[0])]
    status = ["Initial"]
    price = [0.0, 0.0]
    last_index = [0]
    candle_indices = []

    def get_low(curr_idx: int) -> None:
        j = last_index[-1] + 1
        sub = lows[j:curr_idx]
        inx = j + int(np.argmin(sub)) if len(sub) > 0 else j
        val = float(lows[inx])
        low.append(val)
        price.append(val)
        status.append("low")
        last_index.append(inx)
        candle_indices.append(inx)

    def get_high(curr_idx: int) -> None:
        j = last_index[-1] + 1
        sub = highs[j:curr_idx]
        inx = j + int(np.argmax(sub)) if len(sub) > 0 else j
        val = float(highs[inx])
        high.append(val)
        price.append(val)
        status.append("high")
        last_index.append(inx)
        candle_indices.append(inx)

    for i in range(1, n):
        if low[-1] > lows[i]:
            if status[-1] == "low":
                get_high(i)
            val = float(lows[i])
            low.append(val)
            price.append(val)
            status.append("low")
            last_index.append(i)
            candle_indices.append(i)

            if last_index[-2] == last_index[-1]:
                for lst in (price, status, last_index, candle_indices, low):
                    lst.pop(-2)
            if status[-2] == status[-1]:
                for lst in (price, status, last_index, candle_indices):
                    lst.pop(-2)
                high.pop(-1)

        if high[-1] < highs[i]:
            if status[-1] == "high":
                get_low(i)
            val = float(highs[i])
            high.append(val)
            price.append(val)
            status.append("high")
            last_index.append(i)
            candle_indices.append(i)

            if last_index[-2] == last_index[-1]:
                for lst in (price, status, last_index, candle_indices, high):
                    lst.pop(-2)
            if status[-2] == status[-1]:
                for lst in (price, status, last_index, candle_indices):
                    lst.pop(-2)
                low.pop(-1)

    return {c_idx: (1 if pt_status == "high" else 0) for pt_status, c_idx in zip(status[1:], last_index[1:])}


def exact_five_movement_np(ts_arr, open_arr, high_arr, low_arr, close_arr, start_ts, end_ts, req_colour, col_arr=None):
    i0 = int(np.searchsorted(ts_arr, start_ts, side="left"))
    i1 = int(np.searchsorted(ts_arr, end_ts, side="right"))
    L = i1 - i0
    if L < 6:
        return None

    h = high_arr[i0:i1]
    l = low_arr[i0:i1]
    if col_arr is not None:
        cols = col_arr[i0:i1]
    else:
        o = open_arr[i0:i1]
        c = close_arr[i0:i1]
        cols = np.where(c > o, 1, np.where(c < o, 0, -1))

    is_req = cols == req_colour
    cs = np.empty(L + 1, dtype="int32")
    cs[0] = 0
    np.cumsum(is_req, out=cs[1:])

    win5 = (cs[5:L] - cs[0 : L - 5]) == 5
    not_req_after = ~is_req[5:L]
    not_req_before = np.empty(L - 5, dtype=bool)
    not_req_before[0] = True
    not_req_before[1:] = ~is_req[0 : L - 6]

    found = np.flatnonzero(win5 & not_req_after & not_req_before)
    if not found.size:
        return None

    pos = 5 + int(found[0])
    win_h = float(np.max(h[pos - 5 : pos]))
    win_l = float(np.min(l[pos - 5 : pos]))
    return (win_h - win_l) if req_colour == 1 else (win_l - win_h)


def compute_ram_overlays(candle_arrays: tuple, symbol: str, resolution: str, mtf_cache: dict) -> tuple[dict, dict, list[dict]]:
    ts, o, h, l, c, v = candle_arrays
    n = len(ts)
    if n == 0:
        return {}, {}, []

    interval = res_to_seconds(resolution)
    ir_idx_map = detect_r7_states_np(h, l)
    ir_states = {int(ts[idx]): state for idx, state in ir_idx_map.items()}

    col = np.where(c > o, 1, np.where(c < o, 0, -1)).astype("int8")
    cand = np.zeros(n, dtype="int8")
    if n >= 3:
        prev_c, curr_c, next_c = col[:-2], col[1:-1], col[2:]
        cand[1:-1] = ((prev_c != -1) & (prev_c == next_c) & ((curr_c == -1) | (curr_c != prev_c))).astype("int8")

    if n == 1:
        c_idx_arr = np.array([0], dtype="int64")
        ch_cnt = np.array([1], dtype="int64")
    else:
        can_merge = (
            (cand[:-1] == 0)
            & (col[:-1] != -1)
            & (cand[1:] == 0)
            & (col[1:] == col[:-1])
            & ((ts[1:] - ts[:-1]) == interval)
        )
        splits = np.flatnonzero(~can_merge) + 1
        c_idx_arr = np.empty(len(splits) + 1, dtype="int64")
        c_idx_arr[0] = 0
        c_idx_arr[1:] = splits
        ch_cnt = np.empty_like(c_idx_arr)
        ch_cnt[:-1] = c_idx_arr[1:] - c_idx_arr[:-1]
        ch_cnt[-1] = n - c_idx_arr[-1]

    m = len(c_idx_arr)
    ch_ts = ts[c_idx_arr]
    ch_o = o[c_idx_arr]
    ch_c = c[c_idx_arr + ch_cnt - 1]
    ch_col = col[c_idx_arr]
    ch_cand = cand[c_idx_arr].copy()
    ch_h = np.maximum.reduceat(h, c_idx_arr)
    ch_l = np.minimum.reduceat(l, c_idx_arr)
    ch_v = np.add.reduceat(v, c_idx_arr)

    ch_trend = np.full(m, -1, dtype="int8")
    ch_move = np.full(m, np.nan, dtype="float64")
    if m >= 2:
        prelim = ch_cand[:-1] == 1
        up = prelim & (ch_c[1:] > ch_h[:-1])
        dn = prelim & (ch_c[1:] < ch_l[:-1])
        conf = up | dn
        ch_cand[:-1] = conf.astype("int8")
        ch_cand[-1] = 0
        ch_trend[:-1] = np.where(up, 1, np.where(dn, 0, -1))
        ch_move[:-1] = np.where(conf, ch_h[1:] - ch_l[1:], np.nan)
    else:
        ch_cand[:] = 0

    ch_near = np.zeros(m, dtype="int64")
    ch_edge = np.full(m, np.nan, dtype="float64")
    ch_bb_raw = np.zeros(m, dtype="int64")
    ch_bb_chart = np.zeros(m, dtype="int64")
    ch_end_ts = ch_ts + ch_cnt * interval

    for k in np.flatnonzero(ch_cand == 1):
        start_k = k + 2
        if start_k >= m:
            ch_cand[k] = 0
            continue

        tr = ch_trend[k]
        ob_h = ch_h[k]
        ob_l = ch_l[k]

        w1 = min(m, start_k + 64)
        if tr == 1:
            s1 = ch_l[start_k:w1] < ob_h
            if np.any(s1):
                t_pos = start_k + int(np.argmax(s1))
            else:
                s2 = ch_l[w1:] < ob_h
                if not np.any(s2):
                    ch_cand[k] = 0
                    continue
                t_pos = w1 + int(np.argmax(s2))
            if ch_c[t_pos] < ob_l:
                ch_cand[k] = 0
                continue
        else:
            s1 = ch_h[start_k:w1] > ob_l
            if np.any(s1):
                t_pos = start_k + int(np.argmax(s1))
            else:
                s2 = ch_h[w1:] > ob_l
                if not np.any(s2):
                    ch_cand[k] = 0
                    continue
                t_pos = w1 + int(np.argmax(s2))
            if ch_c[t_pos] > ob_h:
                ch_cand[k] = 0
                continue

        near_ts = int(ch_ts[t_pos])
        ch_near[k] = near_ts

        inter_h = float(np.max(ch_h[k + 1 : t_pos + 1]))
        inter_l = float(np.min(ch_l[k + 1 : t_pos + 1]))
        ch_move[k] = inter_h - inter_l
        edge = inter_h if tr == 1 else inter_l
        ch_edge[k] = edge

        chk_start = int(c_idx_arr[t_pos])
        r_start = chk_start + 1
        if r_start >= n:
            ch_cand[k] = 0
            continue

        if tr == 1:
            if c[chk_start] <= ob_l:
                ch_cand[k] = 0
                continue
            cur = r_start
            r_pos = -1
            while cur < n:
                nxt = min(n, cur + 128)
                hit_bb = h[cur:nxt] >= edge
                hit_inv = c[cur:nxt] <= ob_l
                has_bb = bool(np.any(hit_bb))
                has_inv = bool(np.any(hit_inv))
                if has_bb or has_inv:
                    idx_bb = int(np.argmax(hit_bb)) if has_bb else 999999
                    idx_inv = int(np.argmax(hit_inv)) if has_inv else 999999
                    if idx_bb < idx_inv:
                        r_pos = cur + idx_bb
                    break
                cur = nxt
            if r_pos == -1:
                ch_cand[k] = 0
                continue
        else:
            if c[chk_start] >= ob_h:
                ch_cand[k] = 0
                continue
            cur = r_start
            r_pos = -1
            while cur < n:
                nxt = min(n, cur + 128)
                hit_bb = l[cur:nxt] <= edge
                hit_inv = c[cur:nxt] >= ob_h
                has_bb = bool(np.any(hit_bb))
                has_inv = bool(np.any(hit_inv))
                if has_bb or has_inv:
                    idx_bb = int(np.argmax(hit_bb)) if has_bb else 999999
                    idx_inv = int(np.argmax(hit_inv)) if has_inv else 999999
                    if idx_bb < idx_inv:
                        r_pos = cur + idx_bb
                    break
                cur = nxt
            if r_pos == -1:
                ch_cand[k] = 0
                continue

        bb_ts = int(ts[r_pos])
        ch_bb_raw[k] = bb_ts
        c_pos = int(np.searchsorted(ch_ts, bb_ts, side="right")) - 1
        if 0 <= c_pos < m and ch_end_ts[c_pos] > bb_ts:
            ch_bb_chart[k] = int(ch_ts[c_pos])

    ob_by_ts = {}
    confirmed_records = []
    cand_indices = np.flatnonzero(ch_cand == 1)

    if cand_indices.size > 0:
        cand_ts_arr = ch_ts[cand_indices]
        cand_dt_arr = format_kolkata_timestamps(cand_ts_arr)

        for idx_pos, k in enumerate(cand_indices):
            start_t = int(ch_ts[k])
            end_t = int(ch_bb_raw[k])
            req_c = int(ch_trend[k])
            fc_vals = {"1m": None, "3m": None, "5m": None}
            any_conf = False

            for c_res in CONFIRM_RES:
                mtf = mtf_cache.get(c_res)
                if mtf is not None:
                    mv = exact_five_movement_np(*mtf[:5], start_t, end_t, req_c, col_arr=mtf[5])
                    if mv is not None:
                        fc_vals[c_res] = float(mv)
                        any_conf = True

            if not any_conf:
                continue

            rec = {
                "symbol": symbol,
                "resolution": resolution,
                "timestamp": start_t,
                "time": start_t,
                "datetime_kolkata": str(cand_dt_arr[idx_pos]),
                "open": float(ch_o[k]),
                "high": float(ch_h[k]),
                "low": float(ch_l[k]),
                "close": float(ch_c[k]),
                "volume": float(ch_v[k]),
                "colour": int(ch_col[k]) if ch_col[k] != -1 else None,
                "candidate": 1,
                "setup_trend": req_c,
                "setup_movement": None if math.isnan(float(ch_move[k])) else float(ch_move[k]),
                "nearest_candle": int(ch_near[k]),
                "candle_count": int(ch_cnt[k]),
                "price_edge": None if math.isnan(float(ch_edge[k])) else float(ch_edge[k]),
                "breakout_breakdown_candle": end_t,
                "breakout_breakdown_chart_candle": int(ch_bb_chart[k]),
                "five_candle_movement_1m": fc_vals["1m"],
                "five_candle_movement_3m": fc_vals["3m"],
                "five_candle_movement_5m": fc_vals["5m"],
            }
            ob_by_ts[start_t] = rec
            confirmed_records.append(rec)

    return ob_by_ts, ir_states, confirmed_records


def _load_mtf_from_source(symbol: str) -> dict:
    cache = {}
    for r in CONFIRM_RES:
        arrays = get_all_candles_arrays(symbol, r)
        if arrays is not None:
            ts_a, o_a, h_a, l_a, c_a, _ = arrays
            col_a = np.where(c_a > o_a, 1, np.where(c_a < o_a, 0, -1)).astype("int8")
            cache[r] = (ts_a, o_a, h_a, l_a, c_a, col_a)
    return cache


def build_ind_db_for_symbol(symbol: str, resolutions=None, send_live: bool = False) -> int:
    source_db = Path(get_db_path(symbol))
    if not source_db.is_file():
        return 0

    clean_sym = str(symbol).replace(".", "_").replace("-", "_").upper()
    sym_lock = _SYMBOL_LOCKS.setdefault(clean_sym, threading.Lock())
    target_res = resolutions if resolutions is not None else UI_RESOLUTIONS
    mtf_cache = _load_mtf_from_source(clean_sym)

    total_candidates = 0
    built_tables = 0

    with sym_lock:
        for res in target_res:
            candle_data = get_all_candles_arrays(clean_sym, res)
            if candle_data is None or len(candle_data[0]) == 0:
                continue

            ob_by_ts, ir_states, confirmed_records = compute_ram_overlays(
                candle_data, clean_sym, res, mtf_cache
            )
            _RAM_OVERLAY_CACHE[(clean_sym, res)] = (ob_by_ts, ir_states)
            process_ob_alerts(confirmed_records, clean_sym, res, send_live=send_live)

            total_candidates += len(confirmed_records)
            built_tables += 1

    if resolutions is None:
        print(
            f"[RAM Indicator Engine] {clean_sym}: Computed {built_tables} resolutions in RAM "
            f"({total_candidates} final 5C OBs)."
        )
    return total_candidates


def build_all_ind_dbs() -> None:
    t0 = datetime.now()
    print("Computing Order Block + R7 Impulse-Retrace overlays in RAM across all symbols...")
    send_startup_welcome_message(SYMBOL_LIST)
    with ThreadPoolExecutor(max_workers=min(len(SYMBOL_LIST), 4)) as executor:
        list(executor.map(build_ind_db_for_symbol, SYMBOL_LIST))
    elapsed = (datetime.now() - t0).total_seconds()
    print(f"All indicator overlays cached in RAM in {elapsed:.2f}s!")


def save_ind_live_timestamp(timestamp: int, symbol: str, resolution: str) -> None:
    try:
        affected_res = [resolution] + DERIVED_FROM_BASE.get(resolution, [])
        build_ind_db_for_symbol(symbol, resolutions=affected_res, send_live=True)
    except Exception as e:
        print(f"[Indicator Live Update Error | {symbol} {resolution}]: {e}")


def get_ob_chart_data(symbol: str, resolution: str) -> dict:
    clean_sym = str(symbol).replace(".", "_").replace("-", "_").upper()
    raw_candles = get_all_candles(clean_sym, resolution)
    if not raw_candles:
        return {"candles": [], "order_blocks": [], "impulse_retrace": []}

    cached = _RAM_OVERLAY_CACHE.get((clean_sym, resolution))
    if cached is None:
        build_ind_db_for_symbol(clean_sym, resolutions=[resolution], send_live=False)
        cached = _RAM_OVERLAY_CACHE.get((clean_sym, resolution), ({}, {}))

    ob_by_ts, ir_states = cached
    enriched_candles = []
    order_blocks = []
    impulse_points = []
    seen_ts = set()

    for c in raw_candles:
        ts = int(c["time"])
        if ts in seen_ts:
            continue
        seen_ts.add(ts)

        item = {
            "time": ts,
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low": float(c["low"]),
            "close": float(c["close"]),
            "volume": float(c.get("volume", 0.0)),
            "candidate": 0,
        }
        ob = ob_by_ts.get(ts)
        if ob is not None:
            item.update({
                "colour": ob.get("colour"),
                "candidate": 1,
                "setup_trend": ob.get("setup_trend"),
                "setup_movement": ob.get("setup_movement"),
                "nearest_candle": ob.get("nearest_candle"),
                "price_edge": ob.get("price_edge"),
                "breakout_breakdown_candle": ob.get("breakout_breakdown_candle"),
                "breakout_breakdown_chart_candle": ob.get("breakout_breakdown_chart_candle"),
                "five_candle_movement_1m": ob.get("five_candle_movement_1m"),
                "five_candle_movement_3m": ob.get("five_candle_movement_3m"),
                "five_candle_movement_5m": ob.get("five_candle_movement_5m"),
            })
            order_blocks.append(item)

        enriched_candles.append(item)

        ir_val = ir_states.get(ts)
        if ir_val == 1:
            impulse_points.append({"time": ts, "value": float(c["high"])})
        elif ir_val == 0:
            impulse_points.append({"time": ts, "value": float(c["low"])})

    return {
        "candles": enriched_candles,
        "order_blocks": order_blocks,
        "impulse_retrace": impulse_points,
    }


if __name__ == "__main__":
    build_all_ind_dbs()
