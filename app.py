from flask import Flask, request, jsonify
import os
import sqlite3
from datetime import datetime, timezone, timedelta
import requests
import json
import hashlib

app = Flask(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
TE_API_KEY = os.environ.get("TE_API_KEY", "guest:guest")
MEDIASTACK_API_KEY = os.environ.get("MEDIASTACK_API_KEY", "").strip()
CHAT_ID = "-1003759221413"
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()

HIGH_IMPACT_NEWS_TOPIC = 58
MARKET_NEWS_TOPIC = 59
LOT_SIZE_TOPIC = 63
RESULTS_TOPIC = int(os.environ.get("RESULTS_TOPIC", "0") or 0)

TOPIC_MAP = {
    "EURCHF": 3, "AUDCAD": 2, "EURNZD": 14, "EURAUD": 16, "XAUUSD": 17,
    "GBPJPY": 18, "EURUSD": 22, "EURGBP": 25, "AUDJPY": 28, "AUDNZD": 29,
    "XAGUSD": 70, "CADJPY": 72, "EURJPY": 74, "USDCHF": 76, "USDCAD": 78,
    "GBPUSD": 80, "DE40": 82,
}
PROTECTED_TOPICS = set(TOPIC_MAP.values()) | {HIGH_IMPACT_NEWS_TOPIC, MARKET_NEWS_TOPIC}

PAIR_EMOJI = {"XAUUSD": "🥇", "XAGUSD": "🥈", "DE40": "📊"}
PIP_VALUE_MAP = {
    "GBPJPY": 0.4764, "AUDJPY": 0.4749, "CADJPY": 0.4749, "EURJPY": 0.4750,
    "EURAUD": 0.5297, "AUDCAD": 0.5492, "EURGBP": 1.0, "AUDNZD": 0.4405,
    "AUDCHF": 0.9598, "CADCHF": 0.9598, "USDCHF": 0.9598, "EURUSD": 0.7503,
    "EURNZD": 0.4423, "XAUUSD": 0.7445, "XAGUSD": 0.3721, "USDCAD": 0.5484,
    "GBPUSD": 0.7439, "EURCHF": 0.9567,
}
CURRENCY_TO_PAIRS = {
    "EUR": ["EURCHF", "EURNZD", "EURAUD", "EURUSD", "EURGBP", "EURJPY"],
    "AUD": ["AUDCAD", "AUDJPY", "AUDNZD", "EURAUD"],
    "NZD": ["EURNZD", "AUDNZD"],
    "USD": ["XAUUSD", "EURUSD", "USDCHF", "USDCAD", "GBPUSD", "XAGUSD"],
    "GBP": ["GBPJPY", "EURGBP", "GBPUSD"],
    "JPY": ["GBPJPY", "AUDJPY", "CADJPY", "EURJPY"],
    "CAD": ["AUDCAD", "CADJPY", "USDCAD"],
    "CHF": ["EURCHF", "USDCHF"],
}
EVENT_ALIASES = {
    "SETUP": "SETUP", "TP1_HIT": "TP_HIT", "TP_HIT": "TP_HIT",
    "SL_HIT": "SL_HIT", "MOVE_TO_BE": "MOVE_TO_BE", "BE_HIT": "BE_HIT",
    "PAIR_STATS": "PAIR_STATS",
}
NEWS_RULES = {
    "CPI": {"better": "higher", "currency_positive": True},
    "INFLATION": {"better": "higher", "currency_positive": True},
    "CORE CPI": {"better": "higher", "currency_positive": True},
    "NFP": {"better": "higher", "currency_positive": True},
    "NON FARM PAYROLLS": {"better": "higher", "currency_positive": True},
    "GDP": {"better": "higher", "currency_positive": True},
    "RETAIL SALES": {"better": "higher", "currency_positive": True},
    "PMI": {"better": "higher", "currency_positive": True},
    "UNEMPLOYMENT RATE": {"better": "lower", "currency_positive": True},
    "JOBLESS CLAIMS": {"better": "lower", "currency_positive": True},
}
DB_PATH = "signals.db"
VERSION = "ff-signals-bot-v4-market-news"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS trade_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_time_utc TEXT NOT NULL, pair TEXT,
        direction TEXT, event_type TEXT, timeframe TEXT, entry REAL, stop_price REAL,
        stop_pips REAL, target_price REAL, target_pips REAL, risk TEXT, lot_size TEXT, rr TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sent_news_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_key TEXT UNIQUE, sent_at_utc TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS pair_returns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, pair TEXT UNIQUE, risk_pct REAL, profit_pct REAL,
        max_drawdown_pct REAL, days INTEGER, rr REAL, trades INTEGER, updated_at_utc TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS processed_webhooks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id TEXT UNIQUE, event_type TEXT, created_at_utc TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS pending_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id TEXT UNIQUE, pair TEXT NOT NULL,
        payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        created_at_utc TEXT NOT NULL, acknowledged_at_utc TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sent_market_news (
        id INTEGER PRIMARY KEY AUTOINCREMENT, item_key TEXT UNIQUE, sent_at_utc TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sent_market_open (
        id INTEGER PRIMARY KEY AUTOINCREMENT, session_date_key TEXT UNIQUE, sent_at_utc TEXT NOT NULL)""")
    conn.commit(); conn.close()

init_db()

@app.route("/", methods=["GET"])
def home():
    return "TradingView Telegram Bot is running", 200

@app.route("/version", methods=["GET"])
def version():
    return VERSION, 200

def now_utc():
    return datetime.now(timezone.utc)

def api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"

def format_tf(tf) -> str:
    tf = str(tf or "").strip()
    if tf.isdigit(): return f"{tf}m"
    tf_upper = tf.upper()
    if tf_upper in {"D", "W", "M"}: return tf_upper
    return tf

def parse_tv_time(raw_time):
    raw_time = str(raw_time or "").strip()
    if not raw_time: return now_utc()
    if raw_time.isdigit():
        try:
            ts = int(raw_time)
            if ts > 10_000_000_000: ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception: pass
    try:
        dt = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        return (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc))
    except Exception:
        return now_utc()

def parse_calendar_time(raw):
    raw = str(raw or "").strip()
    if not raw: return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc))
    except Exception:
        return None

def format_timestamp(dt) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def get_session(dt) -> str:
    hour = dt.astimezone(timezone.utc).hour
    if 0 <= hour < 7: return "Asia"
    if 7 <= hour < 13: return "London"
    if 13 <= hour < 22: return "New York"
    return "After Hours"

def to_float(value):
    try: return float(str(value).strip().replace(",", ""))
    except Exception: return None

def to_int(value):
    try: return int(float(str(value).strip()))
    except Exception: return None

def normalize_event_type(event_type: str) -> str:
    return EVENT_ALIASES.get(str(event_type or "SETUP").upper().strip(), str(event_type or "SETUP").upper().strip())

def webhook_secret_valid(data: dict) -> bool:
    if not WEBHOOK_SECRET: return True
    return str(data.get("secret", "")).strip() == WEBHOOK_SECRET

def build_signal_id(data: dict) -> str:
    supplied = str(data.get("signal_id", "")).strip()
    if supplied: return supplied
    raw = "|".join([
        str(data.get("pair", "")).upper().strip(),
        str(data.get("direction", "")).upper().strip(),
        normalize_event_type(data.get("event_type", "SETUP")),
        str(data.get("time", "")).strip(),
        str(data.get("entry", "")).strip(),
        str(data.get("stop_price", "")).strip(),
        str(data.get("target_price", "")).strip(),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

def webhook_already_processed(signal_id: str) -> bool:
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed_webhooks WHERE signal_id = ? LIMIT 1", (signal_id,))
    row = cur.fetchone(); conn.close()
    return row is not None

def mark_webhook_processed(signal_id: str, event_type: str):
    conn = get_db(); cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO processed_webhooks (signal_id, event_type, created_at_utc) VALUES (?, ?, ?)",
                (signal_id, event_type, now_utc().isoformat()))
    conn.commit(); conn.close()

def store_pending_signal(data: dict, signal_id: str):
    pair = str(data.get("pair", "")).upper().strip()
    if not pair: return
    payload = {
        "signal_id": signal_id,
        "pair": pair,
        "direction": str(data.get("direction", "")).upper().strip(),
        "event_type": normalize_event_type(data.get("event_type", "SETUP")),
        "timeframe": format_tf(data.get("timeframe", "")),
        "time": str(data.get("time", "")).strip(),
        "entry": data.get("entry"),
        "stop_price": data.get("stop_price"),
        "stop_pips": data.get("stop_pips"),
        "target_price": data.get("target_price"),
        "target_pips": data.get("target_pips"),
        "rr": data.get("rr"),
    }
    conn = get_db(); cur = conn.cursor()
    cur.execute("""INSERT OR REPLACE INTO pending_signals
        (signal_id, pair, payload_json, status, created_at_utc, acknowledged_at_utc)
        VALUES (?, ?, ?, 'pending', ?, NULL)""",
        (signal_id, pair, json.dumps(payload), now_utc().isoformat()))
    conn.commit(); conn.close()

def get_pending_signals(limit: int = 50):
    conn = get_db(); cur = conn.cursor()
    cur.execute("""SELECT * FROM pending_signals WHERE status = 'pending' ORDER BY id ASC LIMIT ?""", (limit,))
    rows = cur.fetchall(); conn.close()
    return rows

def acknowledge_signal(signal_id: str):
    conn = get_db(); cur = conn.cursor()
    cur.execute("""UPDATE pending_signals SET status = 'acknowledged', acknowledged_at_utc = ? WHERE signal_id = ?""",
                (now_utc().isoformat(), signal_id))
    conn.commit(); updated = cur.rowcount; conn.close()
    return updated > 0

def market_item_already_sent(item_key: str) -> bool:
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent_market_news WHERE item_key = ? LIMIT 1", (item_key,))
    row = cur.fetchone(); conn.close()
    return row is not None

def mark_market_item_sent(item_key: str):
    conn = get_db(); cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO sent_market_news (item_key, sent_at_utc) VALUES (?, ?)",
                (item_key, now_utc().isoformat()))
    conn.commit(); conn.close()

def market_open_already_sent(session_date_key: str) -> bool:
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent_market_open WHERE session_date_key = ? LIMIT 1", (session_date_key,))
    row = cur.fetchone(); conn.close()
    return row is not None

def mark_market_open_sent(session_date_key: str):
    conn = get_db(); cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO sent_market_open (session_date_key, sent_at_utc) VALUES (?, ?)",
                (session_date_key, now_utc().isoformat()))
    conn.commit(); conn.close()

def send_telegram_message(text: str, thread_id=None):
    payload = {"chat_id": CHAT_ID, "text": text}
    if thread_id is not None: payload["message_thread_id"] = thread_id
    return requests.post(api_url("sendMessage"), json=payload, timeout=15)

def delete_telegram_message(message_id: int):
    try:
        return requests.post(api_url("deleteMessage"), json={"chat_id": CHAT_ID, "message_id": message_id}, timeout=10)
    except Exception:
        return None

def is_admin(user_id: int) -> bool:
    try:
        resp = requests.post(api_url("getChatMember"), json={"chat_id": CHAT_ID, "user_id": user_id}, timeout=10)
        if resp.status_code != 200: return False
        data = resp.json()
        if not data.get("ok"): return False
        return data["result"].get("status", "") in {"creator", "administrator"}
    except Exception:
        return False

def log_trade_event(event_time_utc, pair, direction, event_type, timeframe, entry, stop_price, stop_pips, target_price, target_pips, risk, lot_size, rr):
    conn = get_db(); cur = conn.cursor()
    cur.execute("""INSERT INTO trade_events (
        event_time_utc, pair, direction, event_type, timeframe, entry, stop_price, stop_pips,
        target_price, target_pips, risk, lot_size, rr) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (event_time_utc, pair, direction, event_type, timeframe, entry, stop_price, stop_pips, target_price, target_pips, risk, lot_size, rr))
    conn.commit(); conn.close()

def fetch_rows_since(start_dt=None):
    conn = get_db(); cur = conn.cursor()
    if start_dt is None:
        cur.execute("SELECT * FROM trade_events ORDER BY event_time_utc ASC, id ASC")
    else:
        cur.execute("SELECT * FROM trade_events WHERE event_time_utc >= ? ORDER BY event_time_utc ASC, id ASC",
                    (start_dt.isoformat(),))
    rows = cur.fetchall(); conn.close()
    return rows

def get_pair_rows(pair):
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM trade_events WHERE pair = ? ORDER BY event_time_utc ASC, id ASC", (pair,))
    rows = cur.fetchall(); conn.close()
    return rows

def summarize_rows(rows):
    total_setups = tp_hits = sl_hits = be_hits = 0
    pips_won = pips_lost = 0.0
    for row in rows:
        event_type = normalize_event_type(row["event_type"])
        target_pips = float(row["target_pips"] or 0.0)
        stop_pips = float(row["stop_pips"] or 0.0)
        if event_type == "SETUP":
            total_setups += 1
        elif event_type == "TP_HIT":
            tp_hits += 1; pips_won += target_pips
        elif event_type == "SL_HIT":
            sl_hits += 1; pips_lost += stop_pips
        elif event_type == "BE_HIT":
            be_hits += 1
    resolved_with_be = tp_hits + sl_hits + be_hits
    resolved_for_winrate = tp_hits + sl_hits
    win_rate = (tp_hits / resolved_for_winrate * 100.0) if resolved_for_winrate > 0 else 0.0
    net_pips = pips_won - pips_lost
    return {
        "total_setups": total_setups, "tp_hits": tp_hits, "sl_hits": sl_hits, "be_hits": be_hits,
        "resolved_with_be": resolved_with_be, "resolved_for_winrate": resolved_for_winrate,
        "win_rate": round(win_rate, 2), "pips_won": round(pips_won, 2),
        "pips_lost": round(pips_lost, 2), "net_pips": round(net_pips, 2),
    }

def build_report(title: str, days=None):
    now = now_utc(); start = None if days is None else now - timedelta(days=days)
    rows = fetch_rows_since(start); stats = summarize_rows(rows)
    period_from = "Start" if start is None else start.strftime("%d %b"); period_to = now.strftime("%d %b")
    report = (
        f"{title}\n\nPeriod:\n{period_from} → {period_to}\n\nTotal Setups: {stats['total_setups']}\n\n"
        f"TP Hits: {stats['tp_hits']}\nSL Hits: {stats['sl_hits']}\nBE Hits: {stats['be_hits']}\n\n"
        f"Win Rate: {stats['win_rate']:.1f}%\n\nPips Won: +{stats['pips_won']:.2f}\n"
        f"Pips Lost: -{stats['pips_lost']:.2f}\n\nNet Pips: {stats['net_pips']:+.2f}"
    )
    return stats, report

def get_latest_signal(pair: str):
    conn = get_db(); cur = conn.cursor()
    cur.execute("""SELECT * FROM trade_events WHERE pair = ? AND event_type = 'SETUP'
                   ORDER BY event_time_utc DESC, id DESC LIMIT 1""", (pair,))
    setup_row = cur.fetchone()
    if not setup_row:
        conn.close(); return None
    setup_time = setup_row["event_time_utc"]; direction = setup_row["direction"]
    cur.execute("""SELECT * FROM trade_events WHERE pair = ? AND direction = ? AND event_time_utc >= ?
                   AND event_type IN ('TP_HIT', 'TP1_HIT', 'SL_HIT', 'BE_HIT', 'MOVE_TO_BE')
                   ORDER BY event_time_utc DESC, id DESC LIMIT 1""", (pair, direction, setup_time))
    latest_followup = cur.fetchone(); conn.close()
    status = "ACTIVE"
    if latest_followup:
        latest_type = normalize_event_type(latest_followup["event_type"])
        if latest_type == "TP_HIT": status = "TP"
        elif latest_type == "SL_HIT": status = "SL"
        elif latest_type == "BE_HIT": status = "BE"
        elif latest_type == "MOVE_TO_BE": status = "MOVE TO BE"
    return {"pair": pair, "direction": setup_row["direction"], "entry": setup_row["entry"],
            "stop_price": setup_row["stop_price"], "target_price": setup_row["target_price"],
            "time": setup_row["event_time_utc"], "status": status}

def build_signal_lookup_message(pair: str) -> str:
    result = get_latest_signal(pair)
    if not result: return f"No saved signal found for {pair}."
    dt = parse_tv_time(result["time"])
    return (f"Latest {pair} Signal\n\nDirection: {result['direction']}\nEntry: {result['entry']}\n"
            f"SL: {result['stop_price']}\nTP: {result['target_price']}\n\nStatus: {result['status']}\n"
            f"Time: {format_timestamp(dt)}")

def get_active_pairs():
    conn = get_db(); cur = conn.cursor()
    cur.execute("""SELECT pair, event_type, event_time_utc, id FROM trade_events
                   WHERE pair IS NOT NULL ORDER BY event_time_utc DESC, id DESC""")
    rows = cur.fetchall(); conn.close()
    latest_by_pair = {}
    for row in rows:
        pair = row["pair"]
        if pair and pair not in latest_by_pair:
            latest_by_pair[pair] = normalize_event_type(row["event_type"])
    return sorted([pair for pair, event_type in latest_by_pair.items() if event_type in {"SETUP", "MOVE_TO_BE"}])

def build_best_pair_message() -> str:
    rows = fetch_rows_since(None)
    if not rows: return "No trade data available yet."
    by_pair = {}
    for row in rows:
        pair = row["pair"]
        if not pair: continue
        by_pair.setdefault(pair, {"tp": 0, "sl": 0, "be": 0, "net_pips": 0.0})
        event_type = normalize_event_type(row["event_type"])
        if event_type == "TP_HIT":
            by_pair[pair]["tp"] += 1; by_pair[pair]["net_pips"] += float(row["target_pips"] or 0.0)
        elif event_type == "SL_HIT":
            by_pair[pair]["sl"] += 1; by_pair[pair]["net_pips"] -= float(row["stop_pips"] or 0.0)
        elif event_type == "BE_HIT":
            by_pair[pair]["be"] += 1
    ranked = list(by_pair.items())
    if not ranked: return "No pair performance data available yet."
    ranked.sort(key=lambda x: x[1]["net_pips"], reverse=True)
    pair, stats = ranked[0]
    resolved_for_winrate = stats["tp"] + stats["sl"]
    win_rate = (stats["tp"] / resolved_for_winrate * 100.0) if resolved_for_winrate > 0 else 0.0
    return (f"🏆 Best Performing Pair\n\n{pair}\n\nTP: {stats['tp']}\nSL: {stats['sl']}\nBE: {stats['be']}\n\n"
            f"Win Rate: {win_rate:.1f}%\nNet Pips: {stats['net_pips']:+.2f}")

def build_ranking_message() -> str:
    rows = fetch_rows_since(None)
    if not rows: return "No pair ranking data available yet."
    by_pair = {}
    for row in rows:
        pair = row["pair"]
        if not pair: continue
        by_pair.setdefault(pair, 0.0)
        event_type = normalize_event_type(row["event_type"])
        if event_type == "TP_HIT": by_pair[pair] += float(row["target_pips"] or 0.0)
        elif event_type == "SL_HIT": by_pair[pair] -= float(row["stop_pips"] or 0.0)
    ranked = sorted(by_pair.items(), key=lambda x: x[1], reverse=True)
    if not ranked: return "No pair ranking data available yet."
    lines = ["📊 Pair Performance\n"]; medals = ["1️⃣", "2️⃣", "3️⃣"]
    for idx, (pair, pips) in enumerate(ranked[:10]):
        prefix = medals[idx] if idx < 3 else f"{idx + 1}."
        lines.append(f"{prefix} {pair}   {pips:+.2f} pips")
    return "\n".join(lines)

def build_live_performance_message(pair: str):
    rows = get_pair_rows(pair)
    if not rows: return None
    stats = summarize_rows(rows); latest = get_latest_signal(pair); status = latest["status"] if latest else "NO SIGNAL"
    return (f"Live Performance\nTP: {stats['tp_hits']}\nSL: {stats['sl_hits']}\nBE: {stats['be_hits']}\n"
            f"Win Rate: {stats['win_rate']:.2f}%\nNet Pips: {stats['net_pips']:+.2f}\nStatus: {status}")

def build_pairstatus_message(pair: str) -> str:
    latest = get_latest_signal(pair); live = build_live_performance_message(pair)
    if not latest and not live: return f"No live data found for {pair}."
    lines = [f"{pair} STATUS\n"]
    if latest:
        lines.append(f"Active Trade: {'YES' if latest['status'] in {'ACTIVE', 'MOVE TO BE'} else 'NO'}")
        lines.append(f"Direction: {latest['direction']}")
        lines.append(f"Entry: {latest['entry']}")
        lines.append(f"TP: {latest['target_price']}")
        lines.append(f"SL: {latest['stop_price']}")
        lines.append("")
    if live: lines.append(live)
    return "\n".join(lines)

def upsert_pair_return(pair, risk_pct, profit_pct, max_drawdown_pct, days, rr, trades):
    conn = get_db(); cur = conn.cursor()
    cur.execute("""INSERT INTO pair_returns (
            pair, risk_pct, profit_pct, max_drawdown_pct, days, rr, trades, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pair) DO UPDATE SET
            risk_pct = excluded.risk_pct, profit_pct = excluded.profit_pct,
            max_drawdown_pct = excluded.max_drawdown_pct, days = excluded.days,
            rr = excluded.rr, trades = excluded.trades, updated_at_utc = excluded.updated_at_utc""",
        (pair, risk_pct, profit_pct, max_drawdown_pct, days, rr, trades, now_utc().isoformat()))
    conn.commit(); conn.close()

def delete_pair_return(pair):
    conn = get_db(); cur = conn.cursor()
    cur.execute("DELETE FROM pair_returns WHERE pair = ?", (pair,))
    deleted = cur.rowcount; conn.commit(); conn.close()
    return deleted > 0

def get_pair_return(pair):
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM pair_returns WHERE pair = ?", (pair,))
    row = cur.fetchone(); conn.close()
    return row

def get_all_pair_returns():
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM pair_returns ORDER BY profit_pct DESC, pair ASC")
    rows = cur.fetchall(); conn.close()
    return rows

def build_return_message(pair: str) -> str:
    row = get_pair_return(pair); live = build_live_performance_message(pair)
    if not row and not live: return f"No return or live data saved for {pair}."
    lines = [f"📊 {pair}\n"]
    if row:
        lines.extend([
            "Expected Return", f"Risk: {row['risk_pct']:.2f}%", f"Profit: {row['profit_pct']:.2f}%",
            f"Max Drawdown: {row['max_drawdown_pct']:.2f}%", f"RR: {row['rr']:.2f}",
            f"Trades: {row['trades']}", f"Days Tested: {row['days']}", "",
        ])
    else:
        lines.extend(["Expected Return", "No expected return data saved.", ""])
    lines.append(live or "Live Performance\nNo live trade data saved.")
    return "\n".join(lines)

def build_expected_returns_message() -> str:
    rows = get_all_pair_returns()
    if not rows: return "No expected return data saved yet."
    lines = ["📊 EXPECTED RETURNS\n"]
    for row in rows[:20]: lines.append(f"{row['pair']}   {row['profit_pct']:.2f}%")
    return "\n".join(lines)

def fetch_calendar():
    url = f"https://api.tradingeconomics.com/calendar?c={TE_API_KEY}"
    resp = requests.get(url, timeout=20); resp.raise_for_status()
    return resp.json()

def extract_currency_country(event):
    currency = str(event.get("Currency", "")).upper().strip()
    country = str(event.get("Country", "")).strip()
    if currency: return currency
    country_map = {"United States": "USD", "Euro Area": "EUR", "United Kingdom": "GBP",
                   "Australia": "AUD", "New Zealand": "NZD", "Canada": "CAD",
                   "Switzerland": "CHF", "Japan": "JPY"}
    return country_map.get(country, "")

def affected_pairs_for_currency(currency):
    return [p for p in CURRENCY_TO_PAIRS.get(currency, []) if p in TOPIC_MAP]

def get_upcoming_news_events(min_importance=2):
    now = now_utc(); events = fetch_calendar(); results = []
    for event in events:
        importance = int(event.get("Importance", 0) or 0)
        if importance < min_importance: continue
        event_dt = parse_calendar_time(event.get("Date"))
        if event_dt is None: continue
        currency = extract_currency_country(event)
        if not currency: continue
        affected_pairs = affected_pairs_for_currency(currency)
        if not affected_pairs: continue
        event_name = str(event.get("Event", "Economic Event")).strip()
        minutes_until = int((event_dt - now).total_seconds() // 60)
        results.append({
            "currency": currency, "event": event_name, "time": event_dt, "minutes_until": minutes_until,
            "affected_pairs": affected_pairs, "importance": importance,
            "actual": event.get("Actual"), "forecast": event.get("Forecast"), "previous": event.get("Previous"),
        })
    results.sort(key=lambda x: x["time"])
    return results

def news_already_sent(event_key):
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent_news_events WHERE event_key = ?", (event_key,))
    row = cur.fetchone(); conn.close()
    return row is not None

def mark_news_sent(event_key):
    conn = get_db(); cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO sent_news_events (event_key, sent_at_utc) VALUES (?, ?)",
                (event_key, now_utc().isoformat()))
    conn.commit(); conn.close()

def build_active_trade_news_block(affected_pairs):
    active_pairs = set(get_active_pairs())
    affected_active = [p for p in affected_pairs if p in active_pairs]
    if not affected_active: return ""
    lines = "\n".join([f"• {p}" for p in affected_active])
    return f"\n\nAffects Active Trades:\n{lines}"

def build_next_news_message():
    items = get_upcoming_news_events(min_importance=2)
    items = [x for x in items if x["minutes_until"] >= 0]
    if not items: return "No upcoming medium or high impact news found."
    item = items[0]
    pair_lines = "\n".join([f"• {p}" for p in item["affected_pairs"]])
    impact_text = "HIGH" if item["importance"] >= 3 else "MEDIUM"
    impact_emoji = "🔴" if item["importance"] >= 3 else "🟠"
    active_block = build_active_trade_news_block(item["affected_pairs"])
    return (f"{impact_emoji} Next {impact_text} Impact News\n\nEvent: {item['event']}\n"
            f"Currency: {item['currency']}\nTime: {format_timestamp(item['time'])}\n"
            f"In: {item['minutes_until']} minutes\n\nAffected Pairs:\n{pair_lines}{active_block}")

def build_todays_news_message():
    items = get_upcoming_news_events(min_importance=2)
    today = now_utc().date()
    today_items = [x for x in items if x["time"].date() == today and x["minutes_until"] >= 0]
    if not today_items: return "No upcoming medium or high impact news found today."
    lines = ["Today's News\n"]; active_pairs = set(get_active_pairs())
    for item in today_items[:10]:
        impact_emoji = "🔴" if item["importance"] >= 3 else "🟠"
        pair_text = ", ".join(item["affected_pairs"])
        active_affected = [p for p in item["affected_pairs"] if p in active_pairs]
        active_text = f"\nActive Trades: {', '.join(active_affected)}" if active_affected else ""
        lines.append(f"{impact_emoji} {item['currency']} {item['event']}\n{format_timestamp(item['time'])} "
                     f"({item['minutes_until']}m)\nPairs: {pair_text}{active_text}\n")
    return "\n".join(lines)

def infer_event_bias(event_name: str, currency: str, actual, forecast):
    if actual in (None, "", "None") or forecast in (None, "", "None"): return None
    actual_num = to_float(actual); forecast_num = to_float(forecast)
    if actual_num is None or forecast_num is None: return None
    matched = None; name_upper = event_name.upper()
    for key, rule in NEWS_RULES.items():
        if key in name_upper:
            matched = rule; break
    if not matched: return None
    positive = actual_num > forecast_num if matched["better"] == "higher" else actual_num < forecast_num
    if actual_num == forecast_num: return "Neutral"
    return f"Bullish {currency}" if positive == matched["currency_positive"] else f"Bearish {currency}"

def post_released_high_impact_news():
    try:
        items = get_upcoming_news_events(min_importance=3); now = now_utc(); sent = []
        for item in items:
            seconds_since = (now - item["time"]).total_seconds()
            if not (0 <= seconds_since <= 1800): continue
            if item["actual"] in (None, "", "None"): continue
            event_key = f"released|{item['currency']}|{item['event']}|{item['time'].isoformat()}"
            if news_already_sent(event_key): continue
            bias = infer_event_bias(item["event"], item["currency"], item["actual"], item["forecast"])
            bias_block = f"\nBias:\n📊 {bias}\n" if bias else ""
            message = (f"🚨 HIGH IMPACT NEWS\n\n{item['currency']} {item['event']}\n\nActual: {item['actual']}\n"
                       f"Forecast: {item['forecast']}\nPrevious: {item['previous']}{bias_block}"
                       f"\nTime: {format_timestamp(item['time'])}")
            tg_resp = send_telegram_message(message, thread_id=HIGH_IMPACT_NEWS_TOPIC)
            if tg_resp.status_code == 200:
                mark_news_sent(event_key); sent.append(item["event"])
        return {"ok": True, "sent_count": len(sent), "sent": sent}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def fetch_mediastack_news(keywords, limit=3):
    if not MEDIASTACK_API_KEY: return []
    try:
        params = {"access_key": MEDIASTACK_API_KEY, "keywords": keywords, "languages": "en",
                  "sort": "published_desc", "limit": limit}
        r = requests.get("http://api.mediastack.com/v1/news", params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        return data.get("data", []) or []
    except Exception:
        return []

def build_market_news_post(prefix: str, title: str, source: str):
    return f"🌐 MARKET NEWS\n\n{prefix}\n\n{title}\n\nSource: {source or 'Unknown'}"

def run_auto_market_news():
    sent = []
    feeds = [
        ("gold OR xauusd OR bullion OR precious metals", "🟡 GOLD UPDATE", "gold"),
        ("oil OR crude OR wti OR brent OR opec", "🛢 OIL UPDATE", "oil"),
        ("iran OR israel OR war OR conflict OR sanctions OR missile", "⚡ MACRO UPDATE", "macro"),
    ]
    for keywords, prefix, tag in feeds:
        items = fetch_mediastack_news(keywords, limit=3)
        for item in items:
            title = str(item.get("title", "")).strip(); source = str(item.get("source", "")).strip()
            item_key = f"{tag}|{title}|{source}"
            if not title or market_item_already_sent(item_key): continue
            msg = build_market_news_post(prefix, title, source)
            tg_resp = send_telegram_message(msg, thread_id=MARKET_NEWS_TOPIC)
            if tg_resp.status_code == 200:
                mark_market_item_sent(item_key); sent.append(item_key)
    return {"ok": True, "sent_count": len(sent), "sent": sent}

def send_market_open(session_name):
    if session_name == "London":
        text = ("🌐 MARKET NEWS\n\n📊 MARKET OPEN\n\nLondon session is live\n\n"
                "→ EUR & GBP pairs active\n→ Expect volatility\n\nFocus:\n⚠️ EURUSD, GBPUSD, EURGBP")
    elif session_name == "New York":
        text = ("🌐 MARKET NEWS\n\n📊 MARKET OPEN\n\nNew York session is live\n\n"
                "→ USD is now in focus\n→ Gold and US assets may become more active\n\n"
                "Focus:\n⚠️ USD pairs and Gold")
    else:
        return False
    return send_telegram_message(text, thread_id=MARKET_NEWS_TOPIC).status_code == 200

def run_market_open_check():
    now = now_utc(); sent = []
    london_key = f"London|{now.strftime('%Y-%m-%d')}"; ny_key = f"NewYork|{now.strftime('%Y-%m-%d')}"
    if now.hour == 7 and not market_open_already_sent(london_key):
        if send_market_open("London"):
            mark_market_open_sent(london_key); sent.append("London")
    if now.hour == 13 and not market_open_already_sent(ny_key):
        if send_market_open("New York"):
            mark_market_open_sent(ny_key); sent.append("New York")
    return {"ok": True, "sent": sent}

def build_signal_message(data):
    pair = str(data.get("pair", "")).upper().strip()
    direction = str(data.get("direction", "")).upper().strip()
    entry = str(data.get("entry", "")).strip()
    stop_price = str(data.get("stop_price", "")).strip()
    stop_pips = str(data.get("stop_pips", "")).strip()
    target_price = str(data.get("target_price", "")).strip()
    target_pips = str(data.get("target_pips", "")).strip()
    rr = str(data.get("rr", "")).strip()
    tf = format_tf(data.get("timeframe", "")); raw_time = str(data.get("time", "")).strip()
    event_type = normalize_event_type(data.get("event_type", "SETUP"))
    dt = parse_tv_time(raw_time); session = get_session(dt); time_text = format_timestamp(dt)
    emoji = "📈" if direction == "BUY" else "📉"; pair_emoji = PAIR_EMOJI.get(pair, "💱")
    if event_type == "SETUP":
        return (f"{emoji} {pair_emoji} {pair} {direction} SETUP\n\nEntry: {entry}\n"
                f"SL: {stop_price} ({stop_pips} pips)\nTP: {target_price} ({target_pips} pips)\n\n"
                f"RR: {rr}:1\nTF: {tf}\nSession: {session}\nTime: {time_text}")
    if event_type == "TP_HIT":
        return (f"✅ {pair_emoji} {pair} TP HIT\n\nDirection: {direction}\nEntry: {entry}\n"
                f"TP: {target_price} ({target_pips} pips)\nTF: {tf}\nTime: {time_text}")
    if event_type == "SL_HIT":
        return (f"❌ {pair_emoji} {pair} SL HIT\n\nDirection: {direction}\nEntry: {entry}\n"
                f"SL: {stop_price} ({stop_pips} pips)\nTF: {tf}\nTime: {time_text}")
    if event_type == "MOVE_TO_BE":
        return (f"🟠 {pair_emoji} {pair} MOVE TO BE\n\nDirection: {direction}\nEntry: {entry}\n"
                f"SL moved to break even\nTF: {tf}\nTime: {time_text}")
    if event_type == "BE_HIT":
        return (f"🔒 {pair_emoji} {pair} BE HIT\n\nDirection: {direction}\nEntry: {entry}\n"
                f"Trade closed at break even\nTF: {tf}\nTime: {time_text}")
    return f"ℹ️ {pair_emoji} {pair} {event_type}\n\nDirection: {direction}\nTF: {tf}\nTime: {time_text}"

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)
        if not webhook_secret_valid(data):
            return jsonify({"ok": False, "error": "Invalid secret"}), 403
        pair = str(data.get("pair", "")).upper().strip()
        event_type = normalize_event_type(data.get("event_type", "SETUP"))
        signal_id = build_signal_id(data)
        if webhook_already_processed(signal_id):
            return jsonify({"ok": True, "status": "duplicate_ignored", "signal_id": signal_id}), 200
        if not pair:
            return jsonify({"ok": False, "error": "Missing pair"}), 400
        if event_type == "PAIR_STATS":
            risk_pct = to_float(data.get("risk_pct")); profit_pct = to_float(data.get("profit_pct"))
            max_dd_pct = to_float(data.get("max_drawdown_pct")); days = to_int(data.get("days"))
            rr = to_float(data.get("rr")); trades = to_int(data.get("trades"))
            if None in (risk_pct, profit_pct, max_dd_pct, days, rr, trades):
                return jsonify({"ok": False, "error": "Missing PAIR_STATS fields"}), 400
            upsert_pair_return(pair, risk_pct, profit_pct, max_dd_pct, days, rr, trades)
            topic = TOPIC_MAP.get(pair); message = build_return_message(pair)
            tg_resp = send_telegram_message(message, thread_id=topic)
            if tg_resp.status_code != 200:
                return jsonify({"ok": False, "error": "Telegram send failed"}), 502
            mark_webhook_processed(signal_id, event_type)
            return jsonify({"ok": True, "status": "pair_stats_saved", "signal_id": signal_id}), 200
        direction = str(data.get("direction", "")).upper().strip()
        timeframe = format_tf(data.get("timeframe", "")); raw_time = str(data.get("time", "")).strip()
        dt = parse_tv_time(raw_time)
        log_trade_event(
            event_time_utc=dt.astimezone(timezone.utc).isoformat(),
            pair=pair, direction=direction, event_type=event_type, timeframe=timeframe,
            entry=to_float(data.get("entry")), stop_price=to_float(data.get("stop_price")),
            stop_pips=to_float(data.get("stop_pips")), target_price=to_float(data.get("target_price")),
            target_pips=to_float(data.get("target_pips")), risk="", lot_size="", rr=str(data.get("rr", "")).strip(),
        )
        if event_type == "SETUP": store_pending_signal(data, signal_id)
        topic = TOPIC_MAP.get(pair); message = build_signal_message(data)
        tg_resp = send_telegram_message(message, thread_id=topic)
        if tg_resp.status_code != 200:
            return jsonify({"ok": False, "error": "Telegram send failed"}), 502
        mark_webhook_processed(signal_id, event_type)
        return jsonify({"ok": True, "status": "sent", "signal_id": signal_id}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/ea/health", methods=["GET"])
def ea_health():
    return jsonify({"ok": True, "version": VERSION, "utc": now_utc().isoformat()}), 200

@app.route("/ea/pending-signals", methods=["GET"])
def ea_pending_signals():
    try:
        limit = to_int(request.args.get("limit", "50")) or 50
        rows = get_pending_signals(limit); signals = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            payload["status"] = row["status"]; payload["created_at_utc"] = row["created_at_utc"]
            signals.append(payload)
        return jsonify({"ok": True, "signals": signals}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/ea/ack", methods=["POST"])
def ea_ack():
    try:
        data = request.get_json(force=True)
        signal_id = str(data.get("signal_id", "")).strip()
        if not signal_id:
            return jsonify({"ok": False, "error": "Missing signal_id"}), 400
        updated = acknowledge_signal(signal_id)
        return jsonify({"ok": True, "acknowledged": updated, "signal_id": signal_id}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/daily-report", methods=["GET"])
def daily_report():
    try:
        stats, report = build_report("📊 DAILY RESULTS", days=1)
        return jsonify({"ok": True, **stats, "report": report}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/weekly-report", methods=["GET"])
def weekly_report():
    try:
        stats, report = build_report("📊 WEEKLY RESULTS", days=7)
        return jsonify({"ok": True, **stats, "report": report}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/weekly-report/send", methods=["GET", "POST"])
def weekly_report_send():
    try:
        if not RESULTS_TOPIC:
            return jsonify({"ok": False, "error": "RESULTS_TOPIC not set"}), 400
        stats, report = build_report("📊 WEEKLY RESULTS", days=7)
        tg_resp = send_telegram_message(report, thread_id=RESULTS_TOPIC)
        if tg_resp.status_code != 200:
            return jsonify({"ok": False, "error": "Telegram send failed"}), 502
        return jsonify({"ok": True, "report_sent": True, **stats}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/check-news", methods=["GET", "POST"])
def check_news():
    try:
        news_items = get_upcoming_news_events(min_importance=3); sent = []
        for item in news_items:
            minutes_until = item["minutes_until"]
            if not (0 <= minutes_until <= 30): continue
            event_key = f"upcoming|{item['currency']}|{item['event']}|{item['time'].isoformat()}"
            if news_already_sent(event_key): continue
            pair_lines = "\n".join([f"• {p}" for p in item["affected_pairs"]])
            active_block = build_active_trade_news_block(item["affected_pairs"])
            message = (f"🔴 HIGH IMPACT NEWS SOON\n\nCurrency: {item['currency']}\nEvent: {item['event']}\n"
                       f"Time: {format_timestamp(item['time'])}\nStarts In: {minutes_until} minutes\n\n"
                       f"Affected Pairs:\n{pair_lines}{active_block}\n\nBe aware of volatility.")
            tg_resp = send_telegram_message(message, thread_id=HIGH_IMPACT_NEWS_TOPIC)
            if tg_resp.status_code == 200:
                mark_news_sent(event_key); sent.append({"currency": item["currency"], "event": item["event"], "minutes_until": minutes_until})
        return jsonify({"ok": True, "sent_count": len(sent), "sent": sent}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/released-news-check", methods=["GET"])
def released_news_check():
    result = post_released_high_impact_news()
    return jsonify(result), (200 if result.get("ok") else 500)

@app.route("/market-open-check", methods=["GET"])
def market_open_check():
    return jsonify(run_market_open_check()), 200

@app.route("/market-news-auto", methods=["GET"])
def market_news_auto():
    return jsonify(run_auto_market_news()), 200

def calculate_lot_size(pair, risk, stop_pips):
    pip_value = PIP_VALUE_MAP.get(pair)
    if pip_value is None: return None, None
    if stop_pips <= 0: return pip_value, None
    return pip_value, risk / (stop_pips * pip_value)

def process_lotsize_command(text):
    parts = text.strip().split()
    if len(parts) != 4:
        return "Lot Size Calculator\n\nUse:\n/lotsize PAIR RISK STOP_PIPS\n\nExample:\n/lotsize AUDCAD 200 25"
    _, pair, risk_raw, stop_raw = parts; pair = pair.upper().strip()
    try:
        risk = float(risk_raw); stop_pips = float(stop_raw)
    except ValueError:
        return "Invalid numbers. Example:\n/lotsize AUDCAD 200 25"
    pip_value, lot_size = calculate_lot_size(pair, risk, stop_pips)
    if pip_value is None: return f"No pip value saved for {pair}."
    if lot_size is None: return "Stop pips must be greater than 0."
    return (f"Lot Size Result\n\nPair: {pair}\nRisk: £{risk:.2f}\nStop: {stop_pips:.2f} pips\n"
            f"Pip Value: {pip_value:.4f}\n\nLot Size: {lot_size:.2f}")

def reset_trade_stats():
    conn = get_db(); cur = conn.cursor()
    cur.execute("DELETE FROM trade_events"); conn.commit(); conn.close()

def reset_pair_stats(pair):
    conn = get_db(); cur = conn.cursor()
    cur.execute("DELETE FROM trade_events WHERE pair = ?", (pair,))
    deleted = cur.rowcount; conn.commit(); conn.close()
    return deleted

@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    try:
        data = request.get_json(force=True)
        message = data.get("message") or data.get("edited_message")
        if not message: return jsonify({"ok": True, "ignored": True}), 200
        text = str(message.get("text", "")).strip()
        chat = message.get("chat", {}); chat_id = str(chat.get("id", ""))
        thread_id = message.get("message_thread_id")
        from_user = message.get("from", {}); user_id = from_user.get("id")
        is_bot_user = from_user.get("is_bot", False); message_id = message.get("message_id")
        if chat_id != CHAT_ID: return jsonify({"ok": True, "ignored": "wrong_chat"}), 200
        if is_bot_user: return jsonify({"ok": True, "ignored": "bot_message"}), 200
        admin = is_admin(user_id) if user_id else False

        if (not admin) and (thread_id in PROTECTED_TOPICS):
            if message_id: delete_telegram_message(message_id)
            return jsonify({"ok": True, "handled": "deleted_non_admin_message"}), 200

        if text.startswith("/"):
            if text.startswith("/help"):
                help_text = (
                    "Available Commands\n\n/help\n/daily\n/weekly\n/stats\n/nextnews\n/todaynews\n"
                    "/marketnews [text]\n/goldupdates [text]\n/oilupdates [text]\n/marketbias [text]\n"
                    "/signal PAIR\n/bestpair\n/ranking\n/pairstatus PAIR\n/return PAIR\n"
                    "/expectedreturns\n/lotsize PAIR RISK STOP_PIPS\n"
                    "/addreturn PAIR RISK PROFIT MAXDD DAYS RR TRADES\n/deletereturn PAIR\n"
                    "/resetpair PAIR\n/resetstats"
                )
                send_telegram_message(help_text, thread_id=thread_id)

            elif text.startswith("/daily"):
                _, report = build_report("📊 DAILY RESULTS", days=1); send_telegram_message(report, thread_id=thread_id)
            elif text.startswith("/weekly"):
                _, report = build_report("📊 WEEKLY RESULTS", days=7); send_telegram_message(report, thread_id=thread_id)
            elif text.startswith("/stats"):
                _, report = build_report("📊 SIGNAL STATISTICS", days=None); send_telegram_message(report, thread_id=thread_id)
            elif text.startswith("/nextnews"):
                send_telegram_message(build_next_news_message(), thread_id=thread_id)
            elif text.startswith("/todaynews") or text.startswith("/todaysnews"):
                send_telegram_message(build_todays_news_message(), thread_id=thread_id)
            elif text.startswith("/marketnews"):
                msg = text.replace("/marketnews", "", 1).strip()
                if not msg:
                    send_telegram_message("Usage:\n/marketnews [text]\n\nExample:\n/marketnews US data strong → USD bullish", thread_id=thread_id)
                else:
                    send_telegram_message(f"🌐 MARKET NEWS\n\n{msg}", thread_id=MARKET_NEWS_TOPIC)
            elif text.startswith("/goldupdates"):
                msg = text.replace("/goldupdates", "", 1).strip()
                if not msg:
                    send_telegram_message("Usage:\n/goldupdates [text]\n\nExample:\n/goldupdates Gold rejecting resistance", thread_id=thread_id)
                else:
                    send_telegram_message(f"🌐 MARKET NEWS\n\n🟡 GOLD UPDATE\n\n{msg}", thread_id=MARKET_NEWS_TOPIC)
            elif text.startswith("/oilupdates"):
                msg = text.replace("/oilupdates", "", 1).strip()
                if not msg:
                    send_telegram_message("Usage:\n/oilupdates [text]\n\nExample:\n/oilupdates Oil rising on supply concerns", thread_id=thread_id)
                else:
                    send_telegram_message(f"🌐 MARKET NEWS\n\n🛢 OIL UPDATE\n\n{msg}", thread_id=MARKET_NEWS_TOPIC)
            elif text.startswith("/marketbias"):
                msg = text.replace("/marketbias", "", 1).strip()
                if not msg:
                    send_telegram_message("Usage:\n/marketbias [text]\n\nExample:\n/marketbias USD bullish, gold bearish", thread_id=thread_id)
                else:
                    send_telegram_message(f"🌐 MARKET NEWS\n\n📊 MARKET BIAS\n\n{msg}", thread_id=MARKET_NEWS_TOPIC)
            elif text.startswith("/bestpair"):
                send_telegram_message(build_best_pair_message(), thread_id=thread_id)
            elif text.startswith("/ranking"):
                send_telegram_message(build_ranking_message(), thread_id=thread_id)
            elif text.startswith("/signal"):
                parts = text.split()
                if len(parts) != 2:
                    send_telegram_message("Usage:\n/signal PAIR\n\nExample:\n/signal EURCHF", thread_id=thread_id)
                else:
                    send_telegram_message(build_signal_lookup_message(parts[1].upper().strip()), thread_id=thread_id)
            elif text.startswith("/pairstatus"):
                parts = text.split()
                if len(parts) != 2:
                    send_telegram_message("Usage:\n/pairstatus PAIR\n\nExample:\n/pairstatus EURCHF", thread_id=thread_id)
                else:
                    send_telegram_message(build_pairstatus_message(parts[1].upper().strip()), thread_id=thread_id)
            elif text.startswith("/expectedreturns"):
                send_telegram_message(build_expected_returns_message(), thread_id=thread_id)
            elif text.startswith("/return"):
                parts = text.split()
                if len(parts) != 2:
                    send_telegram_message("Usage:\n/return PAIR\n\nExample:\n/return EURCHF", thread_id=thread_id)
                else:
                    send_telegram_message(build_return_message(parts[1].upper().strip()), thread_id=thread_id)
            elif text.startswith("/lotsize"):
                if thread_id != LOT_SIZE_TOPIC:
                    send_telegram_message("Use /lotsize inside the LOT SIZE topic.", thread_id=thread_id)
                else:
                    send_telegram_message(process_lotsize_command(text), thread_id=thread_id)
            elif text.startswith("/addreturn"):
                if not admin:
                    send_telegram_message("❌ Admin only command", thread_id=thread_id)
                else:
                    parts = text.split()
                    if len(parts) != 8:
                        send_telegram_message("Usage:\n/addreturn PAIR RISK PROFIT MAXDD DAYS RR TRADES\n\nExample:\n/addreturn EURCHF 1 69.6 4 158 1.6 141", thread_id=thread_id)
                    else:
                        pair = parts[1].upper().strip(); risk_pct = to_float(parts[2]); profit_pct = to_float(parts[3]); max_dd = to_float(parts[4]); days = to_int(parts[5]); rr = to_float(parts[6]); trades = to_int(parts[7])
                        if None in (risk_pct, profit_pct, max_dd, days, rr, trades):
                            send_telegram_message("Invalid /addreturn values.", thread_id=thread_id)
                        else:
                            existed = get_pair_return(pair) is not None
                            upsert_pair_return(pair, risk_pct, profit_pct, max_dd, days, rr, trades)
                            prefix = "♻️ Return updated" if existed else "✅ Return saved"
                            send_telegram_message(f"{prefix}\n\nPair: {pair}\nRisk: {risk_pct:.2f}%\nProfit: {profit_pct:.2f}%\nMax DD: {max_dd:.2f}%\nRR: {rr:.2f}\nTrades: {trades}\nDays Tested: {days}", thread_id=thread_id)
            elif text.startswith("/deletereturn"):
                if not admin:
                    send_telegram_message("❌ Admin only command", thread_id=thread_id)
                else:
                    parts = text.split()
                    if len(parts) != 2:
                        send_telegram_message("Usage:\n/deletereturn PAIR\n\nExample:\n/deletereturn EURCHF", thread_id=thread_id)
                    else:
                        pair = parts[1].upper().strip()
                        send_telegram_message(f"{'🗑 Return removed for ' + pair if delete_pair_return(pair) else 'No return data found for ' + pair + '.'}", thread_id=thread_id)
            elif text.startswith("/resetpair"):
                if not admin:
                    send_telegram_message("❌ Admin only command", thread_id=thread_id)
                else:
                    parts = text.split()
                    if len(parts) == 2:
                        pair = parts[1].upper().strip()
                        send_telegram_message(f"⚠️ Confirm reset for {pair}\n\nThis will delete:\n• live TP/SL/BE results\n• net pips\n• active trade status\n\nExpected return data will NOT be deleted.\n\nConfirm with:\n/resetpair {pair} confirm", thread_id=thread_id)
                    elif len(parts) == 3 and parts[2].lower() == "confirm":
                        pair = parts[1].upper().strip(); deleted = reset_pair_stats(pair)
                        send_telegram_message(f"✅ Live stats reset for {pair}\nDeleted events: {deleted}", thread_id=thread_id)
                    else:
                        send_telegram_message("Usage:\n/resetpair PAIR\n\nThen confirm with:\n/resetpair PAIR confirm", thread_id=thread_id)
            elif text == "/resetstats":
                if not admin:
                    send_telegram_message("❌ Admin only command", thread_id=thread_id)
                else:
                    send_telegram_message("⚠️ Confirm full stats reset\n\nThis will delete ALL live trade history.\n\nConfirm with:\n/resetstats confirm RESET", thread_id=thread_id)
            elif text == "/resetstats confirm RESET":
                if not admin:
                    send_telegram_message("❌ Admin only command", thread_id=thread_id)
                else:
                    reset_trade_stats(); send_telegram_message("✅ All live stats reset", thread_id=thread_id)
            return jsonify({"ok": True, "handled": "command"}), 200
        return jsonify({"ok": True, "ignored": "no_action"}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
