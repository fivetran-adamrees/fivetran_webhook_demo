from flask import Flask, request, jsonify
import json
from datetime import datetime

app = Flask(__name__)

@app.route("/fivetran/webhook", methods=["POST", "OPTIONS"])
@app.route("/fivetran/webhook/", methods=["POST", "OPTIONS"])
def fivetran_webhook():
    if request.method == "OPTIONS":
        return "", 200

    payload = request.get_json(silent=True) or {}

    print("\n" + "=" * 80)
    print(f"received_at: {datetime.utcnow().isoformat()}Z")
    print(f"event: {payload.get('event')}")
    print(f"connector_id: {payload.get('connector_id')}")
    print(f"connector_name: {payload.get('connector_name')}")
    print(f"sync_id: {payload.get('sync_id')}")
    print(f"created: {payload.get('created')}")
    print(f"data: {json.dumps(payload.get('data', {}), indent=2)}")
    print("=" * 80 + "\n")

    return jsonify({"status": "ok"}), 200

@app.route("/", methods=["GET"])
def healthcheck():
    return "Webhook server is running", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)