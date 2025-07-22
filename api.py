from flask import Flask, jsonify
import json
import os

app = Flask(__name__)

@app.route("/")
def health():
    return {"status": "up"}

@app.route("/alarms")
def get_alarms():
    try:
        with open("alarm.json", "r") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/signals")
def get_signals():
    try:
        with open("signals.json", "r") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Ekstra endpointler ekleyebilirsin
@app.route("/coins")
def get_coins():
    try:
        with open("coins.json", "r") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)