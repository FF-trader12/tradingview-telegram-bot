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

@app.route("/webhook", methods=["POST"])
def webhook():

    data = request.json

    pair = data["pair"]
    direction = data["direction"]
    entry = data["entry"]
    stop_price = data["stop_price"]
    stop_pips = data["stop_pips"]
    target_price = data["target_price"]
    target_pips = data["target_pips"]
    risk = data["risk"]
    lot = data["lot_size"]
    rr = data["rr"]
    tf = data["timeframe"]

    topic = TOPIC_MAP.get(pair)

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

    requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={
            "chat_id": CHAT_ID,
            "message_thread_id": topic,
            "text": message
        }
    )

    return jsonify({"status":"ok"})
