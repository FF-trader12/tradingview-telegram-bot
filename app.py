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
    "DE40": 82
}

PAIR_EMOJI = {
    "XAUUSD": "🥇",
    "XAGUSD": "🥈",
    "DE40": "📊"
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
    "EURCHF": 0.9567
}

CURRENCY_TO_PAIRS = {
    "EUR": ["EURCHF", "EURNZD", "EURAUD", "EURUSD", "EURGBP", "EURJPY"],
    "AUD": ["AUDCAD", "AUDJPY", "AUDNZD", "EURAUD"],
    "NZD": ["EURNZD", "AUDNZD"],
    "USD": ["XAUUSD", "EURUSD", "USDCHF", "USDCAD", "GBPUSD", "XAGUSD"],
    "GBP": ["GBPJPY", "EURGBP", "GBPUSD"],
    "JPY": ["GBPJPY", "AUDJPY", "CADJPY", "EURJPY"],
    "CAD": ["AUDCAD", "CADJPY", "USDCAD"],
    "CHF": ["EURCHF", "USDCHF"]
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


def format_tf(tf: str) -> str:
    tf = str(tf).strip()
    if tf.isdigit():
        return f"{tf}m"
    tf_upper = tf.upper()
    if tf_upper in {"D", "W", "M"}:
        return tf_upper
    return tf


def parse_tv_time(raw_time: str):
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
        return datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


def format_timestamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def get_session(dt: datetime) -> str:
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


def send_telegram_message(text: str, thread_id: int = None):
    payload = {
        "chat_id": CHAT_ID,
        "text": text
    }
    if thread_id is not None:
        payload["message_thread_id"] = thread_id

    return requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json=payload,
        timeout=5
    )


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
    rr
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
        rr
    ))
    conn.commit()
    conn.close()


def news_already_sent(event_key: str) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent_news_events WHERE event_key = ?", (event_key,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_news_sent(event_key: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sent_news_events (event_key, sent_at_utc) VALUES (?, ?)",
        (event_key, datetime.now(timezone.utc).isoformat())
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
    event_type = str(data.get("event_type", "SETUP")).upper().strip()

    dt = parse_tv_time(raw_time)
    session = get_session(dt)
    time_text = format_timestamp(dt)

    emoji = "📈" if direction == "BUY" else "📉"
    pair_emoji = PAIR_EMOJI.get(pair, "💱")

    if event_type == "SETUP":
        text = (
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
    elif event_type == "TP_HIT":
        text = (
            f"✅ {pair_emoji} {pair} TP HIT\n\n"
            f"Direction: {direction}\n"
            f"Entry: {entry}\n"
            f"TP: {target_price} ({target_pips} pips)\n"
            f"TF: {tf}\n"
            f"Time: {time_text}"
        )
    elif event_type == "SL_HIT":
        text = (
            f"❌ {pair_emoji} {pair} SL HIT\n\n"
            f"Direction: {direction}\n"
            f"Entry: {entry}\n"
            f"SL: {stop_price} ({stop_pips} pips)\n"
            f"TF: {tf}\n"
            f"Time: {time_text}"
        )
    elif event_type == "BE_HIT":
        text = (
            f"🔒 {pair_emoji} {pair} BE HIT\n\n"
            f"Direction: {direction}\n"
            f"Entry: {entry}\n"
            f"Trade closed at break even\n"
            f"TF: {tf}\n"
            f"Time: {time_text}"
        )
    elif event_type == "MOVE_TO_BE":
        text = (
            f"🟠 {pair_emoji} {pair} MOVE TO BE\n\n"
            f"Direction: {direction}\n"
            f"Entry: {entry}\n"
            f"SL moved to break even\n"
            f"TF: {tf}\n"
            f"Time: {time_text}"
        )
    else:
        text = (
            f"ℹ️ {pair_emoji} {pair} {event_type}\n\n"
            f"Direction: {direction}\n"
            f"TF: {tf}\n"
            f"Time: {time_text}"
        )

    return text


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)

        pair = str(data.get("pair", "")).upper().strip()
        direction = str(data.get("direction", "")).upper().strip()
        event_type = str(data.get("event_type", "SETUP")).upper().strip()
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
            rr=str(data.get("rr", "")).strip()
        )

        tg_resp = send_telegram_message(message, thread_id=topic)

        if tg_resp.status_code != 200:
            return jsonify({
                "ok": False,
                "error": "Telegram send failed",
                "telegram_status": tg_resp.status_code,
                "telegram_response": tg_resp.text
            }), 502

        return jsonify({"ok": True, "status": "sent"}), 200

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def compute_stats(days=7):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM trade_events
        WHERE event_time_utc >= ?
        ORDER BY event_time_utc ASC
    """, (start.isoformat(),))
    rows = cur.fetchall()
    conn.close()

    total_setups = 0
    tp_hits = 0
    sl_hits = 0
    be_hits = 0

    pips_won = 0.0
    pips_lost = 0.0

    for row in rows:
        event_type = row["event_type"]
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
        "period_from": start,
        "period_to": now
    }


def build_report(title: str, days=7):
    stats = compute_stats(days=days)
    period_from = stats["period_from"].strftime("%d %b")
    period_to = stats["period_to"].strftime("%d %b")

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
                "telegram_response": tg_resp.text
            }), 502

        return jsonify({"ok": True, "report_sent": True, **stats}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/daily-report", methods=["GET"])
def daily_report():
    try:
        stats, report = build_report("📊 DAILY RESULTS", days=1)
        return jsonify({"ok": True, **stats, "report": report}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def fetch_calendar():
    url = f"https://api.tradingeconomics.com/calendar?c={TE_API_KEY}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def parse_calendar_time(raw):
    raw = str(raw).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


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
        "Japan": "JPY"
    }
    return country_map.get(country, "")


def affected_pairs_for_currency(currency):
    return CURRENCY_TO_PAIRS.get(currency, [])


@app.route("/check-news", methods=["GET", "POST"])
def check_news():
    try:
        now = datetime.now(timezone.utc)
        events = fetch_calendar()

        sent = []

        for event in events:
            importance = int(event.get("Importance", 0) or 0)
            if importance < 3:
                continue

            event_dt = parse_calendar_time(event.get("Date"))
            if event_dt is None:
                continue

            delta = event_dt - now
            minutes_until = int(delta.total_seconds() // 60)

            if not (0 <= minutes_until <= 30):
                continue

            currency = extract_currency_country(event)
            if not currency:
                continue

            affected_pairs = affected_pairs_for_currency(currency)
            if not affected_pairs:
                continue

            event_name = str(event.get("Event", "Economic Event")).strip()
            event_key = f"{currency}|{event_name}|{event_dt.isoformat()}"

            if news_already_sent(event_key):
                continue

            pair_lines = "\n".join([f"• {p}" for p in affected_pairs])

            message = (
                f"⚠️ HIGH IMPACT NEWS SOON\n\n"
                f"Currency: {currency}\n"
                f"Event: {event_name}\n"
                f"Time: {format_timestamp(event_dt)}\n"
                f"Starts In: {minutes_until} minutes\n\n"
                f"Affected Pairs:\n"
                f"{pair_lines}\n\n"
                f"Be aware of volatility."
            )

            tg_resp = send_telegram_message(message, thread_id=HIGH_IMPACT_NEWS_TOPIC)

            if tg_resp.status_code == 200:
                mark_news_sent(event_key)
                sent.append({
                    "currency": currency,
                    "event": event_name,
                    "minutes_until": minutes_until
                })

        return jsonify({"ok": True, "sent_count": len(sent), "sent": sent}), 200

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def calculate_lot_size(pair: str, risk: float, stop_pips: float):
    pip_value = PIP_VALUE_MAP.get(pair)
    if pip_value is None:
        return None, None

    if stop_pips <= 0:
        return pip_value, None

    lot_size = risk / (stop_pips * pip_value)
    return pip_value, lot_size


def process_lotsize_command(text: str):
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

        if chat_id != CHAT_ID:
            return jsonify({"ok": True, "ignored": "wrong_chat"}), 200

        if text.startswith("/help"):
            help_text = (
                "Available Commands\n\n"
                "/help\n"
                "/daily\n"
                "/weekly\n"
                "/lotsize PAIR RISK STOP_PIPS\n\n"
                "Example:\n"
                "/lotsize EURCHF 200 25"
            )
            send_telegram_message(help_text, thread_id=thread_id)
            return jsonify({"ok": True}), 200

        if text.startswith("/daily"):
            _, report = build_report("📊 DAILY RESULTS", days=1)
            send_telegram_message(report, thread_id=thread_id)
            return jsonify({"ok": True}), 200

        if text.startswith("/weekly"):
            _, report = build_report("📊 WEEKLY RESULTS", days=7)
            send_telegram_message(report, thread_id=thread_id)
            return jsonify({"ok": True}), 200

        if text.startswith("/lotsize"):
            if thread_id != LOT_SIZE_TOPIC:
                send_telegram_message(
                    "Use /lotsize inside the LOT SIZE topic.",
                    thread_id=thread_id
                )
                return jsonify({"ok": True}), 200

            result = process_lotsize_command(text)
            send_telegram_message(result, thread_id=thread_id)
            return jsonify({"ok": True}), 200

        return jsonify({"ok": True, "ignored": "no_command"}), 200

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
