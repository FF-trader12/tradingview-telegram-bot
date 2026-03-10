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
    "TP1_HIT": "TP_HIT",
    "TP_HIT": "TP_HIT",
    "SL_HIT": "SL_HIT",
    "BE_HIT": "BE_HIT",
    "MOVE_TO_BE": "MOVE_TO_BE",
    "SETUP": "SETUP",
}

DB_PATH = "signals.db"


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

    conn.commit()
    conn.close()


init_db()


@app.route("/", methods=["GET"])
def home():
    return "TradingView Telegram Bot is running", 200


def api_url(method):
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"


def format_tf(tf):
    tf = str(tf).strip()
    if tf.isdigit():
        return f"{tf}m"
    tf_upper = tf.upper()
    if tf_upper in {"D", "W", "M"}:
        return tf_upper
    return tf


def parse_tv_time(raw_time):
    raw_time = str(raw_time).strip()
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
    raw = str(raw).strip()
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


def normalize_event_type(event_type):
    event_type = str(event_type or "SETUP").upper().strip()
    return EVENT_ALIASES.get(event_type, event_type)


def send_telegram_message(text, thread_id=None):
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
    }
    if thread_id is not None:
        payload["message_thread_id"] = thread_id

    return requests.post(api_url("sendMessage"), json=payload, timeout=5)


def delete_telegram_message(message_id):
    return requests.post(
        api_url("deleteMessage"),
        json={
            "chat_id": CHAT_ID,
            "message_id": message_id,
        },
        timeout=5,
    )


def is_admin(user_id):
    try:
        resp = requests.post(
            api_url("getChatMember"),
            json={"chat_id": CHAT_ID, "user_id": user_id},
            timeout=5,
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


def log_event(
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
        (event_key, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()


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

    if event_type == "BE_HIT":
        return (
            f"🔒 {pair_emoji} {pair} BE HIT\n\n"
            f"Direction: {direction}\n"
            f"Entry: {entry}\n"
            f"Trade closed at break even\n"
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

    return (
        f"ℹ️ {pair_emoji} {pair} {event_type}\n\n"
        f"Direction: {direction}\n"
        f"TF: {tf}\n"
        f"Time: {time_text}"
    )


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)

        pair = str(data.get("pair", "")).upper().strip()
        direction = str(data.get("direction", "")).upper().strip()
        event_type = normalize_event_type(data.get("event_type", "SETUP"))
        timeframe = format_tf(data.get("timeframe", ""))
        raw_time = str(data.get("time", "")).strip()
        dt = parse_tv_time(raw_time)

        if not pair:
            return jsonify({"ok": False, "error": "Missing pair"}), 400

        topic = TOPIC_MAP.get(pair)
        message = build_signal_message(data)

        log_event(
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

        tg_resp = send_telegram_message(message, thread_id=topic)

        if tg_resp.status_code != 200:
            return jsonify({
                "ok": False,
                "error": "Telegram send failed",
                "telegram_status": tg_resp.status_code,
                "telegram_response": tg_resp.text,
            }), 502

        return jsonify({"ok": True, "status": "sent"}), 200

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
            return jsonify({
                "ok": False,
                "error": "Telegram send failed",
                "telegram_status": tg_resp.status_code,
                "telegram_response": tg_resp.text,
            }), 502

        return jsonify({"ok": True, "report_sent": True, **stats}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def fetch_calendar():
    url = f"https://api.tradingeconomics.com/calendar?c={TE_API_KEY}"
    resp = requests.get(url, timeout=10)
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


def get_high_impact_events():
    now = datetime.now(timezone.utc)
    events = fetch_calendar()
    results = []

    for event in events:
        importance = int(event.get("Importance", 0) or 0)
        if importance < 3:
            continue

        event_dt = parse_calendar_time(event.get("Date"))
        if event_dt is None:
            continue
        if event_dt < now:
            continue

        currency = extract_currency_country(event)
        if not currency:
            continue

        affected_pairs = affected_pairs_for_currency(currency)
        if not affected_pairs:
            continue

        event_name = str(event.get("Event", "Economic Event")).strip()
        minutes_until = int((event_dt - now).total_seconds() // 60)

        results.append({
            "currency": currency,
            "event": event_name,
            "time": event_dt,
            "minutes_until": minutes_until,
            "affected_pairs": affected_pairs,
        })

    results.sort(key=lambda x: x["time"])
    return results


@app.route("/check-news", methods=["GET", "POST"])
def check_news():
    try:
        news_items = get_high_impact_events()
        sent = []

        for item in news_items:
            minutes_until = item["minutes_until"]
            if not (0 <= minutes_until <= 30):
                continue

            event_key = f"{item['currency']}|{item['event']}|{item['time'].isoformat()}"
            if news_already_sent(event_key):
                continue

            pair_lines = "\n".join([f"• {p}" for p in item["affected_pairs"]])

            message = (
                f"⚠️ HIGH IMPACT NEWS SOON\n\n"
                f"Currency: {item['currency']}\n"
                f"Event: {item['event']}\n"
                f"Time: {format_timestamp(item['time'])}\n"
                f"Starts In: {minutes_until} minutes\n\n"
                f"Affected Pairs:\n"
                f"{pair_lines}\n\n"
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
        if latest_type == "TP_HIT":
            status = "TP"
        elif latest_type == "SL_HIT":
            status = "SL"
        elif latest_type == "BE_HIT":
            status = "BE HIT"
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


def build_next_news_message():
    items = get_high_impact_events()
    if not items:
        return "No upcoming high impact news found."

    item = items[0]
    pair_lines = "\n".join([f"• {p}" for p in item["affected_pairs"]])

    return (
        f"Next High Impact News\n\n"
        f"{item['event']}\n"
        f"{item['currency']}\n"
        f"{format_timestamp(item['time'])}\n"
        f"In: {item['minutes_until']} minutes\n\n"
        f"Affected Pairs:\n"
        f"{pair_lines}"
    )


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
                    "/signal PAIR\n"
                    "/bestpair\n"
                    "/lotsize PAIR RISK STOP_PIPS"
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

            elif text.startswith("/bestpair"):
                send_telegram_message(build_best_pair_message(), thread_id=thread_id)

            elif text.startswith("/signal"):
                parts = text.split()
                if len(parts) != 2:
                    send_telegram_message(
                        "Usage:\n/signal PAIR\n\nExample:\n/signal EURCHF",
                        thread_id=thread_id,
                    )
                else:
                    pair = parts[1].upper().strip()
                    send_telegram_message(build_signal_lookup_message(pair), thread_id=thread_id)

            elif text.startswith("/lotsize"):
                if thread_id != LOT_SIZE_TOPIC:
                    send_telegram_message(
                        "Use /lotsize inside the LOT SIZE topic.",
                        thread_id=thread_id,
                    )
                else:
                    result = process_lotsize_command(text)
                    send_telegram_message(result, thread_id=thread_id)

            if (not admin) and (thread_id in PROTECTED_TOPICS) and message_id:
                delete_telegram_message(message_id)

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
