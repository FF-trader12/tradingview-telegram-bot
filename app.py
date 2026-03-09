from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = "-1003759221413"

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
    "AUDNZD": 29
}


@app.route("/", methods=["GET"])
def home():
    return "TradingView Telegram Bot is running", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)

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
        tf = str(data.get("timeframe", "")).strip()

        if not pair:
            return jsonify({"ok": False, "error": "Missing pair"}), 400

        topic = TOPIC_MAP.get(pair)
        if topic is None:
            return jsonify({"ok": False, "error": f"Pair not mapped: {pair}"}), 400

        emoji = "📈" if direction == "BUY" else "📉"

        message = f"""{emoji} {pair} {direction}

Entry: {entry}
SL: {stop_price} ({stop_pips} pips)
TP: {target_price} ({target_pips} pips)

Risk: {risk}
Lot Size: {lot}
RR: {rr}:1
TF: {tf}
"""

        telegram_response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "message_thread_id": topic,
                "text": message
            },
            timeout=2
        )

        if telegram_response.status_code != 200:
            return jsonify({
                "ok": False,
                "error": "Telegram send failed",
                "telegram_status": telegram_response.status_code,
                "telegram_response": telegram_response.text
            }), 502

        return jsonify({"ok": True, "status": "sent"}), 200

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
