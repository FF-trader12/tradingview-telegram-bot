from flask import Flask, request, jsonify
import os
import sqlite3
from datetime import datetime, timezone, timedelta
import requests

app = Flask(__name__)

# ─────────────────────────────
# Config
# ─────────────────────────────
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
VERSION = "ff-signals-bot-v2"


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

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sent_news_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT UNIQUE,
            sent_at_utc TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
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
        """
    )

    conn.commit()
    conn.close()


init_db()


# ─────────────────────────────
# Basic routes / helpers
# ─────────────────────────────
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
    if tf.isdigit():
        return f"{tf}m"
    tf_upper = tf.upper()
    if tf_upper in {"D", "W", "M"}:
        return tf_upper
    return tf


def parse_tv_time(raw_time):
    raw_time = str(raw_time or "").strip()
    if not raw_time:
        return now_utc()

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
        return now_utc()


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


def format_timestamp(dt) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def get_session(dt) -> str:
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


def normalize_event_type(event_type: str) -> str:
    event_type = str(event_type or "SETUP").upper().strip()
    return EVENT_ALIASES.get(event_type, event_type)


# ─────────────────────────────
# Telegram helpers
# ─────────────────────────────
def send_telegram_message(text: str, thread_id=None):
    payload = {"chat_id": CHAT_ID, "text": text}
    if thread_id is not None:
        payload["message_thread_id"] = thread_id
    return requests.post(api_url("sendMessage"), json=payload, timeout=10)


def delete_telegram_message(message_id: int):
    try:
        return requests.post(
            api_url("deleteMessage"),
            json={"chat_id": CHAT_ID, "message_id": message_id},
            timeout=10,
        )
    except Exception:
        return None


def is_admin(user_id: int) -> bool:
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
        return data["result"].get("status", "") in {"creator", "administrator"}
    except Exception:
        return False


# ─────────────────────────────
# Trade event storage / stats
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
    cur.execute(
        """
        INSERT INTO trade_events (
            event_time_utc, pair, direction, event_type, timeframe,
            entry, stop_price, stop_pips, target_price, target_pips,
            risk, lot_size, rr
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
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
        ),
    )
    conn.commit()
    conn.close()


def fetch_rows_since(start_dt=None):
    conn = get_db()
    cur = conn.cursor()
    if start_dt is None:
        cur.execute("SELECT * FROM trade_events ORDER BY event_time_utc ASC, id ASC")
    else:
        cur.execute(
            "SELECT * FROM trade_events WHERE event_time_utc >= ? ORDER BY event_time_utc ASC, id ASC",
            (start_dt.isoformat(),),
        )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_pair_rows(pair):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM trade_events WHERE pair = ? ORDER BY event_time_utc ASC, id ASC",
        (pair,),
    )
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
        target_pips = float(row["target_pips"] or 0.0)
        stop_pips = float(row["stop_pips"] or 0.0)

        if event_type == "SETUP":
            total_setups += 1
        elif event_type == "TP_HIT":
            tp_hits += 1
            pips_won += target_pips
        elif event_type == "SL_HIT":
            sl_hits += 1
            pips_lost += stop_pips
        elif event_type == "BE_HIT":
            be_hits += 1

    resolved_with_be = tp_hits + sl_hits + be_hits
    resolved_for_winrate = tp_hits + sl_hits
    win_rate = (tp_hits / resolved_for_winrate * 100.0) if resolved_for_winrate > 0 else 0.0
    net_pips = pips_won - pips_lost

    return {
        "total_setups": total_setups,
        "tp_hits": tp_hits,
        "sl_hits": sl_hits,
        "be_hits": be_hits,
        "resolved_with_be": resolved_with_be,
        "resolved_for_winrate": resolved_for_winrate,
        "win_rate": round(win_rate, 2),
        "pips_won": round(pips_won, 2),
        "pips_lost": round(pips_lost, 2),
        "net_pips": round(net_pips, 2),
    }


def build_report(title: str, days=None):
    now = now_utc()
    start = None if days is None else now - timedelta(days=days)
    rows = fetch_rows_since(start)
    stats = summarize_rows(rows)

    period_from = "Start" if start is None else start.strftime("%d %b")
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


def get_latest_signal(pair: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM trade_events
        WHERE pair = ? AND event_type = 'SETUP'
        ORDER BY event_time_utc DESC, id DESC
        LIMIT 1
        """,
        (pair,),
    )
    setup_row = cur.fetchone()
    if not setup_row:
        conn.close()
        return None

    setup_time = setup_row["event_time_utc"]
    direction = setup_row["direction"]

    cur.execute(
        """
        SELECT * FROM trade_events
        WHERE pair = ?
          AND direction = ?
          AND event_time_utc >= ?
          AND event_type IN ('TP_HIT', 'TP1_HIT', 'SL_HIT', 'BE_HIT', 'MOVE_TO_BE')
        ORDER BY event_time_utc DESC, id DESC
        LIMIT 1
        """,
        (pair, direction, setup_time),
    )
    latest_followup = cur.fetchone()
    conn.close()

    status = "ACTIVE"
    if latest_followup:
        latest_type = normalize_event_type(latest_followup["event_type"])
        if latest_type == "TP_HIT":
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


def build_signal_lookup_message(pair: str) -> str:
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
    cur.execute(
        """
        SELECT pair, event_type, event_time_utc, id
        FROM trade_events
        WHERE pair IS NOT NULL
        ORDER BY event_time_utc DESC, id DESC
        """
    )
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


def build_best_pair_message() -> str:
    rows = fetch_rows_since(None)
    if not rows:
        return "No trade data available yet."

    by_pair = {}
    for row in rows:
        pair = row["pair"]
        if not pair:
            continue
        if pair not in by_pair:
            by_pair[pair] = {"tp": 0, "sl": 0, "be": 0, "net_pips": 0.0}

        event_type = normalize_event_type(row["event_type"])
        if event_type == "TP_HIT":
            by_pair[pair]["tp"] += 1
            by_pair[pair]["net_pips"] += float(row["target_pips"] or 0.0)
        elif event_type == "SL_HIT":
            by_pair[pair]["sl"] += 1
            by_pair[pair]["net_pips"] -= float(row["stop_pips"] or 0.0)
        elif event_type == "BE_HIT":
            by_pair[pair]["be"] += 1

    ranked = list(by_pair.items())
    if not ranked:
        return "No pair performance data available yet."

    ranked.sort(key=lambda x: x[1]["net_pips"], reverse=True)
    pair, stats = ranked[0]
    resolved_for_winrate = stats["tp"] + stats["sl"]
    win_rate = (stats["tp"] / resolved_for_winrate * 100.0) if resolved_for_winrate > 0 else 0.0

    return (
        f"🏆 Best Performing Pair\n\n"
        f"{pair}\n\n"
        f"TP: {stats['tp']}\n"
        f"SL: {stats['sl']}\n"
        f"BE: {stats['be']}\n\n"
        f"Win Rate: {win_rate:.1f}%\n"
        f"Net Pips: {stats['net_pips']:+.2f}"
    )


def build_ranking_message() -> str:
    rows = fetch_rows_since(None)
    if not rows:
        return "No pair ranking data available yet."

    by_pair = {}
    for row in rows:
        pair = row["pair"]
        if not pair:
            continue
        if pair not in by_pair:
            by_pair[pair] = 0.0
        event_type = normalize_event_type(row["event_type"])
        if event_type == "TP_HIT":
            by_pair[pair] += float(row["target_pips"] or 0.0)
        elif event_type == "SL_HIT":
            by_pair[pair] -= float(row["stop_pips"] or 0.0)

    ranked = sorted(by_pair.items(), key=lambda x: x[1], reverse=True)
    if not ranked:
        return "No pair ranking data available yet."

    lines = ["📊 Pair Performance\n"]
    medals = ["1️⃣", "2️⃣", "3️⃣"]
    for idx, (pair, pips) in enumerate(ranked[:10]):
        prefix = medals[idx] if idx < 3 else f"{idx + 1}."
        lines.append(f"{prefix} {pair}   {pips:+.2f} pips")
    return "\n".join(lines)


def build_live_performance_message(pair: str):
    rows = get_pair_rows(pair)
    if not rows:
        return None

    stats = summarize_rows(rows)
    latest = get_latest_signal(pair)
    status = latest["status"] if latest else "NO SIGNAL"

    return (
        f"Live Performance\n"
        f"TP: {stats['tp_hits']}\n"
        f"SL: {stats['sl_hits']}\n"
        f"BE: {stats['be_hits']}\n"
        f"Win Rate: {stats['win_rate']:.2f}%\n"
        f"Net Pips: {stats['net_pips']:+.2f}\n"
        f"Status: {status}"
    )


def build_pairstatus_message(pair: str) -> str:
    latest = get_latest_signal(pair)
    live = build_live_performance_message(pair)

    if not latest and not live:
        return f"No live data found for {pair}."

    lines = [f"{pair} STATUS\n"]
    if latest:
        lines.append(f"Active Trade: {'YES' if latest['status'] in {'ACTIVE', 'MOVE TO BE'} else 'NO'}")
        lines.append(f"Direction: {latest['direction']}")
        lines.append(f"Entry: {latest['entry']}")
        lines.append(f"TP: {latest['target_price']}")
        lines.append(f"SL: {latest['stop_price']}")
        lines.append("")
    if live:
        lines.append(live)
    return "\n".join(lines)


# ─────────────────────────────
# Return / expectancy storage
# ─────────────────────────────
def upsert_pair_return(pair, risk_pct, profit_pct, max_drawdown_pct, days, rr, trades):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
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
        """,
        (pair, risk_pct, profit_pct, max_drawdown_pct, days, rr, trades, now_utc().isoformat()),
    )
    conn.commit()
    conn.close()


def delete_pair_return(pair):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM pair_returns WHERE pair = ?", (pair,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted > 0


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
    cur.execute("SELECT * FROM pair_returns ORDER BY profit_pct DESC, pair ASC")
    rows = cur.fetchall()
    conn.close()
    return rows


def build_return_message(pair: str) -> str:
    row = get_pair_return(pair)
    live = build_live_performance_message(pair)

    if not row and not live:
        return f"No return or live data saved for {pair}."

    lines = [f"📊 {pair}\n"]

    if row:
        lines.extend([
            "Expected Return",
            f"Risk: {row['risk_pct']:.2f}%",
            f"Profit: {row['profit_pct']:.2f}%",
            f"Max Drawdown: {row['max_drawdown_pct']:.2f}%",
            f"RR: {row['rr']:.2f}",
            f"Trades: {row['trades']}",
            f"Days Tested: {row['days']}",
            "",
        ])
    else:
        lines.extend(["Expected Return", "No expected return data saved.", ""])

    lines.append(live or "Live Performance\nNo live trade data saved.")
    return "\n".join(lines)


def build_returns_message() -> str:
    rows = get_all_pair_returns()
    if not rows:
        return "No expected return data saved yet."

    lines = ["📊 EXPECTED RETURNS\n"]
    for row in rows[:20]:
        lines.append(f"{row['pair']}   {row['profit_pct']:.2f}%")
    return "\n".join(lines)


# ─────────────────────────────
# News system
# ─────────────────────────────
def fetch_calendar():
    url = f"https://api.tradingeconomics.com/calendar?c={TE_API_KEY}"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()


def extract_currency_country(event):
    currency = str(event.get("Currency", "")).upper().strip()
    country = str(event.get("Country", "")).strip()
    if currency:
        return currency

    country_map = {
        "United States": "USD",
        "Euro Area": "EUR",
        "United Kingdom": "GBP",
        "Australia": "AUD",
        "New Zealand": "NZD",
        "Canada": "CAD",
        "Switzerland": "CHF",
        "Japan": "JPY",
    }
    return country_map.get(country, "")


def affected_pairs_for_currency(currency):
    return [p for p in CURRENCY_TO_PAIRS.get(currency, []) if p in TOPIC_MAP]


def get_upcoming_news_events(min_importance=2):
    now = now_utc()
    events = fetch_calendar()
    results = []

    for event in events:
        importance = int(event.get("Importance", 0) or 0)
        if importance < min_importance:
            continue

        event_dt = parse_calendar_time(event.get("Date"))
        if event_dt is None or event_dt < now:
            continue

        currency = extract_currency_country(event)
        if not currency:
            continue

        affected_pairs = affected_pairs_for_currency(currency)
        if not affected_pairs:
            continue

        event_name = str(event.get("Event", "Economic Event")).strip()
        minutes_until = int((event_dt - now).total_seconds() // 60)

        results.append(
            {
                "currency": currency,
                "event": event_name,
                "time": event_dt,
                "minutes_until": minutes_until,
                "affected_pairs": affected_pairs,
                "importance": importance,
            }
        )

    results.sort(key=lambda x: x["time"])
    return results


def news_already_sent(event_key):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent_news_events WHERE event_key = ?", (event_key,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_news_sent(event_key):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sent_news_events (event_key, sent_at_utc) VALUES (?, ?)",
        (event_key, now_utc().isoformat()),
    )
    conn.commit()
    conn.close()


def build_active_trade_news_block(affected_pairs):
    active_pairs = set(get_active_pairs())
    affected_active = [p for p in affected_pairs if p in active_pairs]
    if not affected_active:
        return ""
    lines = "\n".join([f"• {p}" for p in affected_active])
    return f"\n\nAffects Active Trades:\n{lines}"


def build_next_news_message():
    items = get_upcoming_news_events(min_importance=2)
    if not items:
        return "No upcoming medium or high impact news found."

    item = items[0]
    pair_lines = "\n".join([f"• {p}" for p in item["affected_pairs"]])
    impact_text = "HIGH" if item["importance"] >= 3 else "MEDIUM"
    impact_emoji = "🔴" if item["importance"] >= 3 else "🟠"
    active_block = build_active_trade_news_block(item["affected_pairs"])

    return (
        f"{impact_emoji} Next {impact_text} Impact News\n\n"
        f"Event: {item['event']}\n"
        f"Currency: {item['currency']}\n"
        f"Time: {format_timestamp(item['time'])}\n"
        f"In: {item['minutes_until']} minutes\n\n"
        f"Affected Pairs:\n"
        f"{pair_lines}"
        f"{active_block}"
    )


def build_todays_news_message():
    items = get_upcoming_news_events(min_importance=2)
    if not items:
        return "No upcoming medium or high impact news found today."

    today = now_utc().date()
    today_items = [x for x in items if x["time"].date() == today]
    if not today_items:
        return "No upcoming medium or high impact news found today."

    lines = ["Today's News\n"]
    active_pairs = set(get_active_pairs())

    for item in today_items[:10]:
        impact_emoji = "🔴" if item["importance"] >= 3 else "🟠"
        pair_text = ", ".join(item["affected_pairs"])
        active_affected = [p for p in item["affected_pairs"] if p in active_pairs]
        active_text = f"\nActive Trades: {', '.join(active_affected)}" if active_affected else ""

        lines.append(
            f"{impact_emoji} {item['currency']} {item['event']}\n"
            f"{format_timestamp(item['time'])} ({item['minutes_until']}m)\n"
            f"Pairs: {pair_text}{active_text}\n"
        )

    return "\n".join(lines)


def build_check_news_message():
    items = get_upcoming_news_events(min_importance=2)
    alert_items = [x for x in items if 0 <= x["minutes_until"] <= 30]
    if not alert_items:
        return "No medium or high impact news due in the next 30 minutes."

    lines = ["News In Next 30 Minutes\n"]
    active_pairs = set(get_active_pairs())

    for item in alert_items[:10]:
        impact_emoji = "🔴" if item["importance"] >= 3 else "🟠"
        pair_text = ", ".join(item["affected_pairs"])
        active_affected = [p for p in item["affected_pairs"] if p in active_pairs]
        active_text = f"\nActive Trades: {', '.join(active_affected)}" if active_affected else ""

        lines.append(
            f"{impact_emoji} {item['currency']} {item['event']}\n"
            f"{format_timestamp(item['time'])} ({item['minutes_until']}m)\n"
            f"Pairs: {pair_text}{active_text}\n"
        )

    return "\n".join(lines)


# ─────────────────────────────
# Signal message builder
# ─────────────────────────────
def build_signal_message(data):
    pair = str(data.get("pair", "")).upper().strip()
    direction = str(data.get("direction", "")).upper().strip()
    entry = str(data.get("entry", "")).strip()
    stop_price = str(data.get("stop_price", "")).strip()
    stop_pips = str(data.get("stop_pips", "")).strip()
    target_price = str(data.get("target_price", "")).strip()
    target_pips = str(data.get("target_pips", "")).strip()
    risk = str(data.get("risk", "")).strip()
    lot = str(data.get("lot_size", "")).strip()
    rr = str(data.get("rr", "")).strip()
    tf = format_tf(data.get("timeframe", ""))
    raw_time = str(data.get("time", "")).strip()
    event_type = normalize_event_type(data.get("event_type", "SETUP"))

    dt = parse_tv_time(raw_time)
    session = get_session(dt)
    time_text = format_timestamp(dt)
    emoji = "📈" if direction == "BUY" else "📉"
    pair_emoji = PAIR_EMOJI.get(pair, "💱")

    if event_type == "SETUP":
        return (
            f"{emoji} {pair_emoji} {pair} {direction} SETUP\n\n"
            f"Entry: {entry}\n"
            f"SL: {stop_price} ({stop_pips} pips)\n"
            f"TP: {target_price} ({target_pips} pips)\n\n"
            f"Risk: {risk}\n"
            f"Lot Size: {lot}\n"
            f"RR: {rr}:1\n"
            f"TF: {tf}\n"
            f"Session: {session}\n"
            f"Time: {time_text}"
        )

    if event_type == "TP_HIT":
        return (
            f"✅ {pair_emoji} {pair} TP HIT\n\n"
            f"Direction: {direction}\n"
            f"Entry: {entry}\n"
            f"TP: {target_price} ({target_pips} pips)\n"
            f"TF: {tf}\n"
            f"Time: {time_text}"
        )

    if event_type == "SL_HIT":
        return (
            f"❌ {pair_emoji} {pair} SL HIT\n\n"
            f"Direction: {direction}\n"
            f"Entry: {entry}\n"
            f"SL: {stop_price} ({stop_pips} pips)\n"
            f"TF: {tf}\n"
            f"Time: {time_text}"
        )

    if event_type == "MOVE_TO_BE":
        return (
            f"🟠 {pair_emoji} {pair} MOVE TO BE\n\n"
            f"Direction: {direction}\n"
            f"Entry: {entry}\n"
            f"SL moved to break even\n"
            f"TF: {tf}\n"
            f"Time: {time_text}"
        )

    if event_type == "BE_HIT":
        return (
            f"🔒 {pair_emoji} {pair} BE HIT\n\n"
            f"Direction: {direction}\n"
            f"Entry: {entry}\n"
            f"Trade closed at break even\n"
            f"TF: {tf}\n"
            f"Time: {time_text}"
        )

    return (
        f"ℹ️ {pair_emoji} {pair} {event_type}\n\n"
        f"Direction: {direction}\n"
        f"TF: {tf}\n"
        f"Time: {time_text}"
    )


# ─────────────────────────────
# HTTP routes
# ─────────────────────────────
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)
        pair = str(data.get("pair", "")).upper().strip()
        event_type = normalize_event_type(data.get("event_type", "SETUP"))

        if not pair:
            return jsonify({"ok": False, "error": "Missing pair"}), 400

        if event_type == "PAIR_STATS":
            risk_pct = to_float(data.get("risk_pct"))
            profit_pct = to_float(data.get("profit_pct"))
            max_dd_pct = to_float(data.get("max_drawdown_pct"))
            days = to_int(data.get("days"))
            rr = to_float(data.get("rr"))
            trades = to_int(data.get("trades"))

            if None in (risk_pct, profit_pct, max_dd_pct, days, rr, trades):
                return jsonify({"ok": False, "error": "Missing PAIR_STATS fields"}), 400

            upsert_pair_return(pair, risk_pct, profit_pct, max_dd_pct, days, rr, trades)
            topic = TOPIC_MAP.get(pair)
            message = build_return_message(pair)
            tg_resp = send_telegram_message(message, thread_id=topic)
            if tg_resp.status_code != 200:
                return jsonify({"ok": False, "error": "Telegram send failed"}), 502
            return jsonify({"ok": True, "status": "pair_stats_saved"}), 200

        direction = str(data.get("direction", "")).upper().strip()
        timeframe = format_tf(data.get("timeframe", ""))
        raw_time = str(data.get("time", "")).strip()
        dt = parse_tv_time(raw_time)

        log_trade_event(
            event_time_utc=dt.astimezone(timezone.utc).isoformat(),
            pair=pair,
            direction=direction,
            event_type=event_type,
            timeframe=timeframe,
            entry=to_float(data.get("entry")),
            stop_price=to_float(data.get("stop_price")),
            stop_pips=to_float(data.get("stop_pips")),
            target_price=to_float(data.get("target_price")),
            target_pips=to_float(data.get("target_pips")),
            risk=str(data.get("risk", "")).strip(),
            lot_size=str(data.get("lot_size", "")).strip(),
            rr=str(data.get("rr", "")).strip(),
        )

        topic = TOPIC_MAP.get(pair)
        message = build_signal_message(data)
        tg_resp = send_telegram_message(message, thread_id=topic)
        if tg_resp.status_code != 200:
            return jsonify({"ok": False, "error": "Telegram send failed"}), 502

        return jsonify({"ok": True, "status": "sent"}), 200
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
        stats, report = build_report("📊 WEEKLY RESULTS", days=7)
        tg_resp = send_telegram_message(report, thread_id=WEEKLY_RESULTS_TOPIC)
        if tg_resp.status_code != 200:
            return jsonify({"ok": False, "error": "Telegram send failed"}), 502
        return jsonify({"ok": True, "report_sent": True, **stats}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/check-news", methods=["GET", "POST"])
def check_news():
    try:
        news_items = get_upcoming_news_events(min_importance=3)
        sent = []

        for item in news_items:
            minutes_until = item["minutes_until"]
            if not (0 <= minutes_until <= 30):
                continue

            event_key = f"{item['currency']}|{item['event']}|{item['time'].isoformat()}"
            if news_already_sent(event_key):
                continue

            pair_lines = "\n".join([f"• {p}" for p in item["affected_pairs"]])
            active_block = build_active_trade_news_block(item["affected_pairs"])

            message = (
                f"🔴 HIGH IMPACT NEWS SOON\n\n"
                f"Currency: {item['currency']}\n"
                f"Event: {item['event']}\n"
                f"Time: {format_timestamp(item['time'])}\n"
                f"Starts In: {minutes_until} minutes\n\n"
                f"Affected Pairs:\n"
                f"{pair_lines}"
                f"{active_block}\n\n"
                f"Be aware of volatility."
            )

            tg_resp = send_telegram_message(message, thread_id=HIGH_IMPACT_NEWS_TOPIC)
            if tg_resp.status_code == 200:
                mark_news_sent(event_key)
                sent.append({
                    "currency": item["currency"],
                    "event": item["event"],
                    "minutes_until": minutes_until,
                })

        return jsonify({"ok": True, "sent_count": len(sent), "sent": sent}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ─────────────────────────────
# Lot size / resets
# ─────────────────────────────
def calculate_lot_size(pair, risk, stop_pips):
    pip_value = PIP_VALUE_MAP.get(pair)
    if pip_value is None:
        return None, None
    if stop_pips <= 0:
        return pip_value, None
    lot_size = risk / (stop_pips * pip_value)
    return pip_value, lot_size


def process_lotsize_command(text):
    parts = text.strip().split()
    if len(parts) != 4:
        return (
            "Lot Size Calculator\n\n"
            "Use:\n"
            "/lotsize PAIR RISK STOP_PIPS\n\n"
            "Example:\n"
            "/lotsize AUDCAD 200 25"
        )

    _, pair, risk_raw, stop_raw = parts
    pair = pair.upper().strip()

    try:
        risk = float(risk_raw)
        stop_pips = float(stop_raw)
    except ValueError:
        return "Invalid numbers. Example:\n/lotsize AUDCAD 200 25"

    pip_value, lot_size = calculate_lot_size(pair, risk, stop_pips)
    if pip_value is None:
        return f"No pip value saved for {pair}."
    if lot_size is None:
        return "Stop pips must be greater than 0."

    return (
        f"Lot Size Result\n\n"
        f"Pair: {pair}\n"
        f"Risk: £{risk:.2f}\n"
        f"Stop: {stop_pips:.2f} pips\n"
        f"Pip Value: {pip_value:.4f}\n\n"
        f"Lot Size: {lot_size:.2f}"
    )


def reset_trade_stats():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM trade_events")
    conn.commit()
    conn.close()


def reset_pair_stats(pair):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM trade_events WHERE pair = ?", (pair,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted


# ─────────────────────────────
# Telegram command webhook
# ─────────────────────────────
@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    try:
        data = request.get_json(force=True)
        message = data.get("message") or data.get("edited_message")
        if not message:
            return jsonify({"ok": True, "ignored": True}), 200

        text = str(message.get("text", "")).strip()
        chat = message.get("chat", {})
        chat_id = str(chat.get("id", ""))
        thread_id = message.get("message_thread_id")
        from_user = message.get("from", {})
        user_id = from_user.get("id")
        is_bot_user = from_user.get("is_bot", False)
        message_id = message.get("message_id")

        if chat_id != CHAT_ID:
            return jsonify({"ok": True, "ignored": "wrong_chat"}), 200
        if is_bot_user:
            return jsonify({"ok": True, "ignored": "bot_message"}), 200

        admin = is_admin(user_id) if user_id else False

        if text.startswith("/"):
            if text.startswith("/help"):
                help_text = (
                    "Available Commands\n\n"
                    "/help\n"
                    "/daily\n"
                    "/weekly\n"
                    "/stats\n"
                    "/nextnews\n"
                    "/todaysnews\n"
                    "/checknews\n"
                    "/signal PAIR\n"
                    "/bestpair\n"
                    "/ranking\n"
                    "/pairstatus PAIR\n"
                    "/return PAIR\n"
                    "/returns\n"
                    "/lotsize PAIR RISK STOP_PIPS\n"
                    "/addreturn PAIR RISK PROFIT MAXDD DAYS RR TRADES\n"
                    "/deletereturn PAIR\n"
                    "/resetpair PAIR\n"
                    "/resetstats"
                )
                send_telegram_message(help_text, thread_id=thread_id)

            elif text.startswith("/daily"):
                _, report = build_report("📊 DAILY RESULTS", days=1)
                send_telegram_message(report, thread_id=thread_id)

            elif text.startswith("/weekly"):
                _, report = build_report("📊 WEEKLY RESULTS", days=7)
                send_telegram_message(report, thread_id=thread_id)

            elif text.startswith("/stats"):
                _, report = build_report("📊 SIGNAL STATISTICS", days=None)
                send_telegram_message(report, thread_id=thread_id)

            elif text.startswith("/nextnews"):
                send_telegram_message(build_next_news_message(), thread_id=thread_id)

            elif text.startswith("/todaysnews"):
                send_telegram_message(build_todays_news_message(), thread_id=thread_id)

            elif text.startswith("/checknews"):
                send_telegram_message(build_check_news_message(), thread_id=thread_id)

            elif text.startswith("/bestpair"):
                send_telegram_message(build_best_pair_message(), thread_id=thread_id)

            elif text.startswith("/ranking"):
                send_telegram_message(build_ranking_message(), thread_id=thread_id)

            elif text.startswith("/signal"):
                parts = text.split()
                if len(parts) != 2:
                    send_telegram_message("Usage:\n/signal PAIR\n\nExample:\n/signal EURCHF", thread_id=thread_id)
                else:
                    pair = parts[1].upper().strip()
                    send_telegram_message(build_signal_lookup_message(pair), thread_id=thread_id)

            elif text.startswith("/pairstatus"):
                parts = text.split()
                if len(parts) != 2:
                    send_telegram_message("Usage:\n/pairstatus PAIR\n\nExample:\n/pairstatus EURCHF", thread_id=thread_id)
                else:
                    pair = parts[1].upper().strip()
                    send_telegram_message(build_pairstatus_message(pair), thread_id=thread_id)

            elif text.startswith("/returns"):
                send_telegram_message(build_returns_message(), thread_id=thread_id)

            elif text.startswith("/return"):
                parts = text.split()
                if len(parts) != 2:
                    send_telegram_message("Usage:\n/return PAIR\n\nExample:\n/return EURCHF", thread_id=thread_id)
                else:
                    pair = parts[1].upper().strip()
                    send_telegram_message(build_return_message(pair), thread_id=thread_id)

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
                        send_telegram_message(
                            "Usage:\n/addreturn PAIR RISK PROFIT MAXDD DAYS RR TRADES\n\nExample:\n/addreturn EURCHF 1 69.6 4 158 1.6 141",
                            thread_id=thread_id,
                        )
                    else:
                        pair = parts[1].upper().strip()
                        risk_pct = to_float(parts[2])
                        profit_pct = to_float(parts[3])
                        max_dd = to_float(parts[4])
                        days = to_int(parts[5])
                        rr = to_float(parts[6])
                        trades = to_int(parts[7])
                        if None in (risk_pct, profit_pct, max_dd, days, rr, trades):
                            send_telegram_message("Invalid /addreturn values.", thread_id=thread_id)
                        else:
                            existed = get_pair_return(pair) is not None
                            upsert_pair_return(pair, risk_pct, profit_pct, max_dd, days, rr, trades)
                            prefix = "♻️ Return updated" if existed else "✅ Return saved"
                            send_telegram_message(
                                f"{prefix}\n\n"
                                f"Pair: {pair}\n"
                                f"Risk: {risk_pct:.2f}%\n"
                                f"Profit: {profit_pct:.2f}%\n"
                                f"Max DD: {max_dd:.2f}%\n"
                                f"RR: {rr:.2f}\n"
                                f"Trades: {trades}\n"
                                f"Days Tested: {days}",
                                thread_id=thread_id,
                            )

            elif text.startswith("/deletereturn"):
                if not admin:
                    send_telegram_message("❌ Admin only command", thread_id=thread_id)
                else:
                    parts = text.split()
                    if len(parts) != 2:
                        send_telegram_message("Usage:\n/deletereturn PAIR\n\nExample:\n/deletereturn EURCHF", thread_id=thread_id)
                    else:
                        pair = parts[1].upper().strip()
                        if delete_pair_return(pair):
                            send_telegram_message(f"🗑 Return removed for {pair}", thread_id=thread_id)
                        else:
                            send_telegram_message(f"No return data found for {pair}.", thread_id=thread_id)

            elif text.startswith("/resetpair"):
                if not admin:
                    send_telegram_message("❌ Admin only command", thread_id=thread_id)
                else:
                    parts = text.split()
                    if len(parts) == 2:
                        pair = parts[1].upper().strip()
                        send_telegram_message(
                            f"⚠️ Confirm reset for {pair}\n\n"
                            f"This will delete:\n"
                            f"• live TP/SL/BE results\n"
                            f"• net pips\n"
                            f"• active trade status\n\n"
                            f"Expected return data will NOT be deleted.\n\n"
                            f"Confirm with:\n/resetpair {pair} confirm",
                            thread_id=thread_id,
                        )
                    elif len(parts) == 3 and parts[2].lower() == "confirm":
                        pair = parts[1].upper().strip()
                        deleted = reset_pair_stats(pair)
                        send_telegram_message(f"✅ Live stats reset for {pair}\nDeleted events: {deleted}", thread_id=thread_id)
                    else:
                        send_telegram_message("Usage:\n/resetpair PAIR\n\nThen confirm with:\n/resetpair PAIR confirm", thread_id=thread_id)

            elif text == "/resetstats":
                if not admin:
                    send_telegram_message("❌ Admin only command", thread_id=thread_id)
                else:
                    send_telegram_message(
                        "⚠️ Confirm full stats reset\n\n"
                        "This will delete ALL live trade history.\n\n"
                        "Confirm with:\n/resetstats confirm RESET",
                        thread_id=thread_id,
                    )

            elif text == "/resetstats confirm RESET":
                if not admin:
                    send_telegram_message("❌ Admin only command", thread_id=thread_id)
                else:
                    reset_trade_stats()
                    send_telegram_message("✅ All live stats reset", thread_id=thread_id)

            return jsonify({"ok": True, "handled": "command"}), 200

        if (not admin) and (thread_id in PROTECTED_TOPICS):
            if message_id:
                delete_telegram_message(message_id)
            return jsonify({"ok": True, "handled": "deleted_non_admin_message"}), 200

        return jsonify({"ok": True, "ignored": "no_action"}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
