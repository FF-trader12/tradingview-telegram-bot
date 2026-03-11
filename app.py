from flask import Flask, request, jsonify
import requests
import os
import sqlite3
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
TE_API_KEY = os.environ.get("TE_API_KEY", "guest:guest")
CHAT_ID = "-1003759221413"

HIGH_IMPACT_NEWS_TOPIC = 58
WEEKLY_RESULTS_TOPIC = 59
LOT_SIZE_TOPIC = 63

TOPIC_MAP = {
    "EURCHF": 3,
    "AUDCAD": 2,
    "EURNZD": 14,
    "EURAUD": 16,
    "XAUUSD": 17,
    "GBPJPY": 18,
    "EURUSD": 22,
    "EURGBP": 25,
    "AUDJPY": 28,
    "AUDNZD": 29,
    "XAGUSD": 70,
    "CADJPY": 72,
    "EURJPY": 74,
    "USDCHF": 76,
    "USDCAD": 78,
    "GBPUSD": 80,
    "DE40": 82,
}

PROTECTED_TOPICS = set(TOPIC_MAP.values()) | {
    HIGH_IMPACT_NEWS_TOPIC,
    WEEKLY_RESULTS_TOPIC,
}

PAIR_EMOJI = {
    "XAUUSD": "🥇",
    "XAGUSD": "🥈",
    "DE40": "📊",
}

PIP_VALUE_MAP = {
    "GBPJPY": 0.4764,
    "AUDJPY": 0.4749,
    "CADJPY": 0.4749,
    "EURJPY": 0.4750,
    "EURAUD": 0.5297,
    "AUDCAD": 0.5492,
    "EURGBP": 1.0,
    "AUDNZD": 0.4405,
    "AUDCHF": 0.9598,
    "CADCHF": 0.9598,
    "USDCHF": 0.9598,
    "EURUSD": 0.7503,
    "EURNZD": 0.4423,
    "XAUUSD": 0.7445,
    "XAGUSD": 0.3721,
    "USDCAD": 0.5484,
    "GBPUSD": 0.7439,
    "EURCHF": 0.9567,
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
    "SETUP": "SETUP",
    "TP1_HIT": "TP_HIT",
    "TP_HIT": "TP_HIT",
    "SL_HIT": "SL_HIT",
    "MOVE_TO_BE": "MOVE_TO_BE",
    "BE_HIT": "BE_HIT",
    "PAIR_STATS": "PAIR_STATS",
}

DB_PATH = "signals.db"


# ─────────────────────────────
# Database
# ─────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time_utc TEXT NOT NULL,
            pair TEXT,
            direction TEXT,
            event_type TEXT,
            timeframe TEXT,
            entry REAL,
            stop_price REAL,
            stop_pips REAL,
            target_price REAL,
            target_pips REAL,
            risk TEXT,
            lot_size TEXT,
            rr TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_news_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT UNIQUE,
            sent_at_utc TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pair_returns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT UNIQUE,
            risk_pct REAL,
            profit_pct REAL,
            max_drawdown_pct REAL,
            days INTEGER,
            rr REAL,
            trades INTEGER,
            updated_at_utc TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


init_db()


# ─────────────────────────────
# Basic app
# ─────────────────────────────
@app.route("/", methods=["GET"])
def home():
    return "TradingView Telegram Bot is running", 200


def api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"


# ─────────────────────────────
# Helpers
# ─────────────────────────────
def format_tf(tf):
    tf = str(tf or "").strip()
    if tf.isdigit():
        return f"{tf}m"
    tf_upper = tf.upper()
    if tf_upper in {"D", "W", "M"}:
        return tf_upper
    return tf


def parse_tv_time(raw_time):
    raw_time = str(raw_time or "").strip()
    if not raw_time:
        return datetime.now(timezone.utc)

    if raw_time.isdigit():
        try:
            ts = int(raw_time)
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            pass

    try:
        dt = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def parse_calendar_time(raw):
    raw = str(raw or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def format_timestamp(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def get_session(dt):
    hour = dt.astimezone(timezone.utc).hour
    if 0 <= hour < 7:
        return "Asia"
    if 7 <= hour < 13:
        return "London"
    if 13 <= hour < 22:
        return "New York"
    return "After Hours"


def to_float(value):
    try:
        return float(str(value).strip())
    except Exception:
        return None


def to_int(value):
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def normalize_event_type(event_type):
    event_type = str(event_type or "SETUP").upper().strip()
    return EVENT_ALIASES.get(event_type, event_type)


# ─────────────────────────────
# Telegram utilities
# ─────────────────────────────
def send_telegram_message(text, thread_id=None):
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
    }
    if thread_id is not None:
        payload["message_thread_id"] = thread_id

    return requests.post(api_url("sendMessage"), json=payload, timeout=10)


def delete_telegram_message(message_id):
    try:
        return requests.post(
            api_url("deleteMessage"),
            json={"chat_id": CHAT_ID, "message_id": message_id},
            timeout=10,
        )
    except Exception:
        return None


def is_admin(user_id):
    try:
        resp = requests.post(
            api_url("getChatMember"),
            json={"chat_id": CHAT_ID, "user_id": user_id},
            timeout=10,
        )
        if resp.status_code != 200:
            return False
        data = resp.json()
        if not data.get("ok"):
            return False
        status = data["result"].get("status", "")
        return status in {"creator", "administrator"}
    except Exception:
        return False


# ─────────────────────────────
# Trade events
# ─────────────────────────────
def log_trade_event(
    event_time_utc,
    pair,
    direction,
    event_type,
    timeframe,
    entry,
    stop_price,
    stop_pips,
    target_price,
    target_pips,
    risk,
    lot_size,
    rr,
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trade_events (
            event_time_utc, pair, direction, event_type, timeframe,
            entry, stop_price, stop_pips, target_price, target_pips,
            risk, lot_size, rr
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event_time_utc,
        pair,
        direction,
        event_type,
        timeframe,
        entry,
        stop_price,
        stop_pips,
        target_price,
        target_pips,
        risk,
        lot_size,
        rr,
    ))
    conn.commit()
    conn.close()


def fetch_rows_since(start_dt=None):
    conn = get_db()
    cur = conn.cursor()

    if start_dt is None:
        cur.execute("SELECT * FROM trade_events ORDER BY event_time_utc ASC")
    else:
        cur.execute("""
            SELECT * FROM trade_events
            WHERE event_time_utc >= ?
            ORDER BY event_time_utc ASC
        """, (start_dt.isoformat(),))

    rows = cur.fetchall()
    conn.close()
    return rows


def summarize_rows(rows):
    total_setups = 0
    tp_hits = 0
    sl_hits = 0
    be_hits = 0
    pips_won = 0.0
    pips_lost = 0.0

    for row in rows:
        event_type = normalize_event_type(row["event_type"])
        target_pips = row["target_pips"] or 0.0
        stop_pips = row["stop_pips"] or 0.0

        if event_type == "SETUP":
            total_setups += 1
        elif event_type == "TP_HIT":
            tp_hits += 1
            pips_won += float(target_pips)
        elif event_type == "SL_HIT":
            sl_hits += 1
            pips_lost += float(stop_pips)
        elif event_type == "BE_HIT":
            be_hits += 1

    resolved = tp_hits + sl_hits + be_hits
    win_rate = (tp_hits / resolved * 100.0) if resolved > 0 else 0.0
    net_pips = pips_won - pips_lost

    return {
        "total_setups": total_setups,
        "tp_hits": tp_hits,
        "sl_hits": sl_hits,
        "be_hits": be_hits,
        "win_rate": round(win_rate, 2),
        "pips_won": round(pips_won, 2),
        "pips_lost": round(pips_lost, 2),
        "net_pips": round(net_pips, 2),
    }


def build_report(title, days=None):
    now = datetime.now(timezone.utc)
    start = None if days is None else now - timedelta(days=days)

    rows = fetch_rows_since(start)
    stats = summarize_rows(rows)

    if start is None:
        period_from = "Start"
    else:
        period_from = start.strftime("%d %b")
    period_to = now.strftime("%d %b")

    report = (
        f"{title}\n\n"
        f"Period:\n"
        f"{period_from} → {period_to}\n\n"
        f"Total Setups: {stats['total_setups']}\n\n"
        f"TP Hits: {stats['tp_hits']}\n"
        f"SL Hits: {stats['sl_hits']}\n"
        f"BE Hits: {stats['be_hits']}\n\n"
        f"Win Rate: {stats['win_rate']:.1f}%\n\n"
        f"Pips Won: +{stats['pips_won']:.2f}\n"
        f"Pips Lost: -{stats['pips_lost']:.2f}\n\n"
        f"Net Pips: {stats['net_pips']:+.2f}"
    )
    return stats, report


def get_latest_signal(pair):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM trade_events
        WHERE pair = ? AND event_type = 'SETUP'
        ORDER BY event_time_utc DESC
        LIMIT 1
    """, (pair,))
    setup_row = cur.fetchone()

    if not setup_row:
        conn.close()
        return None

    setup_time = setup_row["event_time_utc"]
    direction = setup_row["direction"]

    cur.execute("""
        SELECT * FROM trade_events
        WHERE pair = ?
          AND direction = ?
          AND event_time_utc >= ?
          AND event_type IN ('TP_HIT', 'TP1_HIT', 'SL_HIT', 'BE_HIT', 'MOVE_TO_BE')
        ORDER BY event_time_utc DESC
        LIMIT 1
    """, (pair, direction, setup_time))
    latest_followup = cur.fetchone()

    conn.close()

    status = "ACTIVE"
    if latest_followup:
        latest_type = normalize_event_type(latest_followup["event_type"])
        if latest_type == "TP":
            status = "TP"
        elif latest_type == "TP_HIT":
            status = "TP"
        elif latest_type == "SL_HIT":
            status = "SL"
        elif latest_type == "BE_HIT":
            status = "BE"
        elif latest_type == "MOVE_TO_BE":
            status = "MOVE TO BE"

    return {
        "pair": pair,
        "direction": setup_row["direction"],
        "entry": setup_row["entry"],
        "stop_price": setup_row["stop_price"],
        "target_price": setup_row["target_price"],
        "time": setup_row["event_time_utc"],
        "status": status,
    }


def build_signal_lookup_message(pair):
    result = get_latest_signal(pair)
    if not result:
        return f"No saved signal found for {pair}."

    dt = parse_tv_time(result["time"])
    return (
        f"Latest {pair} Signal\n\n"
        f"Direction: {result['direction']}\n"
        f"Entry: {result['entry']}\n"
        f"SL: {result['stop_price']}\n"
        f"TP: {result['target_price']}\n\n"
        f"Status: {result['status']}\n"
        f"Time: {format_timestamp(dt)}"
    )


def get_active_pairs():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT pair, event_type, event_time_utc
        FROM trade_events
        WHERE pair IS NOT NULL
        ORDER BY event_time_utc DESC, id DESC
    """)
    rows = cur.fetchall()
    conn.close()

    latest_by_pair = {}
    for row in rows:
        pair = row["pair"]
        if pair and pair not in latest_by_pair:
            latest_by_pair[pair] = normalize_event_type(row["event_type"])

    active_pairs = []
    for pair, event_type in latest_by_pair.items():
        if event_type in {"SETUP", "MOVE_TO_BE"}:
            active_pairs.append(pair)

    return sorted(active_pairs)


def build_best_pair_message():
    rows = fetch_rows_since(None)
    if not rows:
        return "No trade data available yet."

    by_pair = {}

    for row in rows:
        pair = row["pair"]
        if not pair:
            continue

        if pair not in by_pair:
            by_pair[pair] = {
                "tp": 0,
                "sl": 0,
                "be": 0,
                "net_pips": 0.0,
                "resolved": 0,
            }

        event_type = normalize_event_type(row["event_type"])

        if event_type == "TP_HIT":
            by_pair[pair]["tp"] += 1
            by_pair[pair]["resolved"] += 1
            by_pair[pair]["net_pips"] += float(row["target_pips"] or 0.0)
        elif event_type == "SL_HIT":
            by_pair[pair]["sl"] += 1
            by_pair[pair]["resolved"] += 1
            by_pair[pair]["net_pips"] -= float(row["stop_pips"] or 0.0)
        elif event_type == "BE_HIT":
            by_pair[pair]["be"] += 1
            by_pair[pair]["resolved"] += 1

    ranked = [(pair, stats) for pair, stats in by_pair.items() if stats["resolved"] > 0]
    if not ranked:
        return "No resolved pair performance data available yet."

    ranked.sort(key=lambda x: x[1]["net_pips"], reverse=True)
    pair, stats = ranked[0]
    win_rate = (stats["tp"] / stats["resolved"] * 100.0) if stats["resolved"] > 0 else 0.0

    return (
        f"Best Performing Pair\n\n"
        f"{pair}\n\n"
        f"TP: {stats['tp']}\n"
        f"SL: {stats['sl']}\n"
        f"BE: {stats['be']}\n\n"
        f"Win Rate: {win_rate:.1f}%\n"
        f"Net Pips: {stats['net_pips']:+.2f}"
    )


# ─────────────────────────────
# Pair returns / backtest stats
# ─────────────────────────────
def upsert_pair_return(pair, risk_pct, profit_pct, max_drawdown_pct, days, rr, trades):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pair_returns (
            pair, risk_pct, profit_pct, max_drawdown_pct, days, rr, trades, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pair) DO UPDATE SET
            risk_pct = excluded.risk_pct,
            profit_pct = excluded.profit_pct,
            max_drawdown_pct = excluded.max_drawdown_pct,
            days = excluded.days,
            rr = excluded.rr,
            trades = excluded.trades,
            updated_at_utc = excluded.updated_at_utc
    """, (
        pair,
        risk_pct,
        profit_pct,
        max_drawdown_pct,
        days,
        rr,
        trades,
        datetime.now(timezone.utc).isoformat(),
    ))
    conn.commit()
    conn.close()


def get_pair_return(pair):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pair_returns WHERE pair = ?", (pair,))
    row = cur.fetchone()
    conn.close()
    return row


def get_all_pair_returns():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM pair_returns
        ORDER BY profit_pct DESC, pair ASC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def build_return_message(pair):
    row = get_pair_return(pair)
    if not row:
        return f"No backtest return data saved yet for {pair}."

    return (
        f"📊 {pair} EXPECTED RETURN\n\n"
        f"Risk: {row['risk_pct']:.2f}%\n"
        f"Profit: {row['profit_pct']:.2f}%\n"
        f"Max Drawdown: {row['max_drawdown_pct']:.2f}%\n"
        f"Days: {row['days']}\n"
        f"RR: {row['rr']:.2f}\n"
        f"Trades: {row['trades']}"
    )

def build_returns_message():
    rows = get_all_pair_returns()
    if not rows:
        return "No expected return data saved yet."

    lines = ["📊 EXPECTED RETURNS\n"]
    for row in rows[:20]:
        lines.append(f"{row['pair']}   {row['profit_pct']:.2f}%")
    return "\n".join(lines)
    from flask import Flask, request, jsonify
import requests
import os
import sqlite3
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
TE_API_KEY = os.environ.get("TE_API_KEY", "guest:guest")
CHAT_ID = "-1003759221413"

HIGH_IMPACT_NEWS_TOPIC = 58
WEEKLY_RESULTS_TOPIC = 59
LOT_SIZE_TOPIC = 63

TOPIC_MAP = {
    "EURCHF": 3,
    "AUDCAD": 2,
    "EURNZD": 14,
    "EURAUD": 16,
    "XAUUSD": 17,
    "GBPJPY": 18,
    "EURUSD": 22,
    "EURGBP": 25,
    "AUDJPY": 28,
    "AUDNZD": 29,
    "XAGUSD": 70,
    "CADJPY": 72,
    "EURJPY": 74,
    "USDCHF": 76,
    "USDCAD": 78,
    "GBPUSD": 80,
    "DE40": 82,
}

PROTECTED_TOPICS = set(TOPIC_MAP.values()) | {
    HIGH_IMPACT_NEWS_TOPIC,
    WEEKLY_RESULTS_TOPIC,
}

PAIR_EMOJI = {
    "XAUUSD": "🥇",
    "XAGUSD": "🥈",
    "DE40": "📊",
}

PIP_VALUE_MAP = {
    "GBPJPY": 0.4764,
    "AUDJPY": 0.4749,
    "CADJPY": 0.4749,
    "EURJPY": 0.4750,
    "EURAUD": 0.5297,
    "AUDCAD": 0.5492,
    "EURGBP": 1.0,
    "AUDNZD": 0.4405,
    "AUDCHF": 0.9598,
    "CADCHF": 0.9598,
    "USDCHF": 0.9598,
    "EURUSD": 0.7503,
    "EURNZD": 0.4423,
    "XAUUSD": 0.7445,
    "XAGUSD": 0.3721,
    "USDCAD": 0.5484,
    "GBPUSD": 0.7439,
    "EURCHF": 0.9567,
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
    "SETUP": "SETUP",
    "TP1_HIT": "TP_HIT",
    "TP_HIT": "TP_HIT",
    "SL_HIT": "SL_HIT",
    "MOVE_TO_BE": "MOVE_TO_BE",
    "BE_HIT": "BE_HIT",
    "PAIR_STATS": "PAIR_STATS",
}

DB_PATH = "signals.db"


# ─────────────────────────────
# Database
# ─────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time_utc TEXT NOT NULL,
            pair TEXT,
            direction TEXT,
            event_type TEXT,
            timeframe TEXT,
            entry REAL,
            stop_price REAL,
            stop_pips REAL,
            target_price REAL,
            target_pips REAL,
            risk TEXT,
            lot_size TEXT,
            rr TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_news_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT UNIQUE,
            sent_at_utc TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pair_returns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT UNIQUE,
            risk_pct REAL,
            profit_pct REAL,
            max_drawdown_pct REAL,
            days INTEGER,
            rr REAL,
            trades INTEGER,
            updated_at_utc TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


init_db()


# ─────────────────────────────
# Basic app
# ─────────────────────────────
@app.route("/", methods=["GET"])
def home():
    return "TradingView Telegram Bot is running", 200


def api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"


# ─────────────────────────────
# Helpers
# ─────────────────────────────
def format_tf(tf):
    tf = str(tf or "").strip()
    if tf.isdigit():
        return f"{tf}m"
    tf_upper = tf.upper()
    if tf_upper in {"D", "W", "M"}:
        return tf_upper
    return tf


def parse_tv_time(raw_time):
    raw_time = str(raw_time or "").strip()
    if not raw_time:
        return datetime.now(timezone.utc)

    if raw_time.isdigit():
        try:
            ts = int(raw_time)
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            pass

    try:
        dt = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def parse_calendar_time(raw):
    raw = str(raw or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def format_timestamp(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def get_session(dt):
    hour = dt.astimezone(timezone.utc).hour
    if 0 <= hour < 7:
        return "Asia"
    if 7 <= hour < 13:
        return "London"
    if 13 <= hour < 22:
        return "New York"
    return "After Hours"


def to_float(value):
    try:
        return float(str(value).strip())
    except Exception:
        return None


def to_int(value):
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def normalize_event_type(event_type):
    event_type = str(event_type or "SETUP").upper().strip()
    return EVENT_ALIASES.get(event_type, event_type)


# ─────────────────────────────
# Telegram utilities
# ─────────────────────────────
def send_telegram_message(text, thread_id=None):
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
    }
    if thread_id is not None:
        payload["message_thread_id"] = thread_id

    return requests.post(api_url("sendMessage"), json=payload, timeout=10)


def delete_telegram_message(message_id):
    try:
        return requests.post(
            api_url("deleteMessage"),
            json={"chat_id": CHAT_ID, "message_id": message_id},
            timeout=10,
        )
    except Exception:
        return None


def is_admin(user_id):
    try:
        resp = requests.post(
            api_url("getChatMember"),
            json={"chat_id": CHAT_ID, "user_id": user_id},
            timeout=10,
        )
        if resp.status_code != 200:
            return False
        data = resp.json()
        if not data.get("ok"):
            return False
        status = data["result"].get("status", "")
        return status in {"creator", "administrator"}
    except Exception:
        return False


# ─────────────────────────────
# Trade events
# ─────────────────────────────
def log_trade_event(
    event_time_utc,
    pair,
    direction,
    event_type,
    timeframe,
    entry,
    stop_price,
    stop_pips,
    target_price,
    target_pips,
    risk,
    lot_size,
    rr,
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trade_events (
            event_time_utc, pair, direction, event_type, timeframe,
            entry, stop_price, stop_pips, target_price, target_pips,
            risk, lot_size, rr
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event_time_utc,
        pair,
        direction,
        event_type,
        timeframe,
        entry,
        stop_price,
        stop_pips,
        target_price,
        target_pips,
        risk,
        lot_size,
        rr,
    ))
    conn.commit()
    conn.close()


def fetch_rows_since(start_dt=None):
    conn = get_db()
    cur = conn.cursor()

    if start_dt is None:
        cur.execute("SELECT * FROM trade_events ORDER BY event_time_utc ASC")
    else:
        cur.execute("""
            SELECT * FROM trade_events
            WHERE event_time_utc >= ?
            ORDER BY event_time_utc ASC
        """, (start_dt.isoformat(),))

    rows = cur.fetchall()
    conn.close()
    return rows


def summarize_rows(rows):
    total_setups = 0
    tp_hits = 0
    sl_hits = 0
    be_hits = 0
    pips_won = 0.0
    pips_lost = 0.0

    for row in rows:
        event_type = normalize_event_type(row["event_type"])
        target_pips = row["target_pips"] or 0.0
        stop_pips = row["stop_pips"] or 0.0

        if event_type == "SETUP":
            total_setups += 1
        elif event_type == "TP_HIT":
            tp_hits += 1
            pips_won += float(target_pips)
        elif event_type == "SL_HIT":
            sl_hits += 1
            pips_lost += float(stop_pips)
        elif event_type == "BE_HIT":
            be_hits += 1

    resolved = tp_hits + sl_hits + be_hits
    win_rate = (tp_hits / resolved * 100.0) if resolved > 0 else 0.0
    net_pips = pips_won - pips_lost

    return {
        "total_setups": total_setups,
        "tp_hits": tp_hits,
        "sl_hits": sl_hits,
        "be_hits": be_hits,
        "win_rate": round(win_rate, 2),
        "pips_won": round(pips_won, 2),
        "pips_lost": round(pips_lost, 2),
        "net_pips": round(net_pips, 2),
    }


def build_report(title, days=None):
    now = datetime.now(timezone.utc)
    start = None if days is None else now - timedelta(days=days)

    rows = fetch_rows_since(start)
    stats = summarize_rows(rows)

    if start is None:
        period_from = "Start"
    else:
        period_from = start.strftime("%d %b")
    period_to = now.strftime("%d %b")

    report = (
        f"{title}\n\n"
        f"Period:\n"
        f"{period_from} → {period_to}\n\n"
        f"Total Setups: {stats['total_setups']}\n\n"
        f"TP Hits: {stats['tp_hits']}\n"
        f"SL Hits: {stats['sl_hits']}\n"
        f"BE Hits: {stats['be_hits']}\n\n"
        f"Win Rate: {stats['win_rate']:.1f}%\n\n"
        f"Pips Won: +{stats['pips_won']:.2f}\n"
        f"Pips Lost: -{stats['pips_lost']:.2f}\n\n"
        f"Net Pips: {stats['net_pips']:+.2f}"
    )
    return stats, report


def get_latest_signal(pair):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM trade_events
        WHERE pair = ? AND event_type = 'SETUP'
        ORDER BY event_time_utc DESC
        LIMIT 1
    """, (pair,))
    setup_row = cur.fetchone()

    if not setup_row:
        conn.close()
        return None

    setup_time = setup_row["event_time_utc"]
    direction = setup_row["direction"]

    cur.execute("""
        SELECT * FROM trade_events
        WHERE pair = ?
          AND direction = ?
          AND event_time_utc >= ?
          AND event_type IN ('TP_HIT', 'TP1_HIT', 'SL_HIT', 'BE_HIT', 'MOVE_TO_BE')
        ORDER BY event_time_utc DESC
        LIMIT 1
    """, (pair, direction, setup_time))
    latest_followup = cur.fetchone()

    conn.close()

    status = "ACTIVE"
    if latest_followup:
        latest_type = normalize_event_type(latest_followup["event_type"])
        if latest_type == "TP":
            status = "TP"
        elif latest_type == "TP_HIT":
            status = "TP"
        elif latest_type == "SL_HIT":
            status = "SL"
        elif latest_type == "BE_HIT":
            status = "BE"
        elif latest_type == "MOVE_TO_BE":
            status = "MOVE TO BE"

    return {
        "pair": pair,
        "direction": setup_row["direction"],
        "entry": setup_row["entry"],
        "stop_price": setup_row["stop_price"],
        "target_price": setup_row["target_price"],
        "time": setup_row["event_time_utc"],
        "status": status,
    }


def build_signal_lookup_message(pair):
    result = get_latest_signal(pair)
    if not result:
        return f"No saved signal found for {pair}."

    dt = parse_tv_time(result["time"])
    return (
        f"Latest {pair} Signal\n\n"
        f"Direction: {result['direction']}\n"
        f"Entry: {result['entry']}\n"
        f"SL: {result['stop_price']}\n"
        f"TP: {result['target_price']}\n\n"
        f"Status: {result['status']}\n"
        f"Time: {format_timestamp(dt)}"
    )


def get_active_pairs():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT pair, event_type, event_time_utc
        FROM trade_events
        WHERE pair IS NOT NULL
        ORDER BY event_time_utc DESC, id DESC
    """)
    rows = cur.fetchall()
    conn.close()

    latest_by_pair = {}
    for row in rows:
        pair = row["pair"]
        if pair and pair not in latest_by_pair:
            latest_by_pair[pair] = normalize_event_type(row["event_type"])

    active_pairs = []
    for pair, event_type in latest_by_pair.items():
        if event_type in {"SETUP", "MOVE_TO_BE"}:
            active_pairs.append(pair)

    return sorted(active_pairs)


def build_best_pair_message():
    rows = fetch_rows_since(None)
    if not rows:
        return "No trade data available yet."

    by_pair = {}

    for row in rows:
        pair = row["pair"]
        if not pair:
            continue

        if pair not in by_pair:
            by_pair[pair] = {
                "tp": 0,
                "sl": 0,
                "be": 0,
                "net_pips": 0.0,
                "resolved": 0,
            }

        event_type = normalize_event_type(row["event_type"])

        if event_type == "TP_HIT":
            by_pair[pair]["tp"] += 1
            by_pair[pair]["resolved"] += 1
            by_pair[pair]["net_pips"] += float(row["target_pips"] or 0.0)
        elif event_type == "SL_HIT":
            by_pair[pair]["sl"] += 1
            by_pair[pair]["resolved"] += 1
            by_pair[pair]["net_pips"] -= float(row["stop_pips"] or 0.0)
        elif event_type == "BE_HIT":
            by_pair[pair]["be"] += 1
            by_pair[pair]["resolved"] += 1

    ranked = [(pair, stats) for pair, stats in by_pair.items() if stats["resolved"] > 0]
    if not ranked:
        return "No resolved pair performance data available yet."

    ranked.sort(key=lambda x: x[1]["net_pips"], reverse=True)
    pair, stats = ranked[0]
    win_rate = (stats["tp"] / stats["resolved"] * 100.0) if stats["resolved"] > 0 else 0.0

    return (
        f"Best Performing Pair\n\n"
        f"{pair}\n\n"
        f"TP: {stats['tp']}\n"
        f"SL: {stats['sl']}\n"
        f"BE: {stats['be']}\n\n"
        f"Win Rate: {win_rate:.1f}%\n"
        f"Net Pips: {stats['net_pips']:+.2f}"
    )


# ─────────────────────────────
# Pair returns / backtest stats
# ─────────────────────────────
def upsert_pair_return(pair, risk_pct, profit_pct, max_drawdown_pct, days, rr, trades):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pair_returns (
            pair, risk_pct, profit_pct, max_drawdown_pct, days, rr, trades, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pair) DO UPDATE SET
            risk_pct = excluded.risk_pct,
            profit_pct = excluded.profit_pct,
            max_drawdown_pct = excluded.max_drawdown_pct,
            days = excluded.days,
            rr = excluded.rr,
            trades = excluded.trades,
            updated_at_utc = excluded.updated_at_utc
    """, (
        pair,
        risk_pct,
        profit_pct,
        max_drawdown_pct,
        days,
        rr,
        trades,
        datetime.now(timezone.utc).isoformat(),
    ))
    conn.commit()
    conn.close()


def get_pair_return(pair):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pair_returns WHERE pair = ?", (pair,))
    row = cur.fetchone()
    conn.close()
    return row


def get_all_pair_returns():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM pair_returns
        ORDER BY profit_pct DESC, pair ASC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def build_return_message(pair):
    row = get_pair_return(pair)
    if not row:
        return f"No backtest return data saved yet for {pair}."

    return (
        f"📊 {pair} EXPECTED RETURN\n\n"
        f"Risk: {row['risk_pct']:.2f}%\n"
        f"Profit: {row['profit_pct']:.2f}%\n"
        f"Max Drawdown: {row['max_drawdown_pct']:.2f}%\n"
        f"Days: {row['days']}\n"
        f"RR: {row['rr']:.2f}\n"
        f"Trades: {row['trades']}"
    )


def build_returns_message():
    rows = get_all_pair_returns()
    if not rows:
        return "No expected return data saved yet."

    lines = ["📊 EXPECTED RETURNS\n"]
    for row in rows[:20]:
        lines.append(f"{row['pair']}   {row['profit_pct']:.2f}%")
    return "\n".join(lines)
