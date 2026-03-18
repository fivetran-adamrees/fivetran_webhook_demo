from flask import Flask, request, jsonify
import json
import os
from datetime import datetime, timezone

import psycopg

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg.connect(DATABASE_URL)


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                create table if not exists fivetran_webhook_events (
                    id bigserial primary key,
                    received_at timestamptz not null default now(),
                    event text not null,
                    created_at timestamptz,
                    connector_id text,
                    connector_name text,
                    connector_type text,
                    sync_id text,
                    destination_group_id text,
                    status text,
                    raw_payload jsonb not null
                );
            """)
        conn.commit()


def parse_timestamp(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


@app.route("/", methods=["GET"])
def healthcheck():
    return "Webhook server is running", 200


@app.route("/init-db", methods=["POST"])
def initialize_database():
    try:
        init_db()
        return jsonify({"status": "ok", "message": "database initialized"}), 200
    except Exception as e:
        print(f"init_db failed: {repr(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/fivetran/webhook", methods=["POST", "OPTIONS"])
@app.route("/fivetran/webhook/", methods=["POST", "OPTIONS"])
def fivetran_webhook():
    if request.method == "OPTIONS":
        return "", 200

    payload = request.get_json(silent=True) or {}

    try:
        event = payload.get("event")
        created_at = parse_timestamp(payload.get("created"))
        connector_id = payload.get("connector_id")
        connector_name = payload.get("connector_name")
        connector_type = payload.get("connector_type")
        sync_id = payload.get("sync_id")
        destination_group_id = payload.get("destination_group_id")
        status = (payload.get("data") or {}).get("status")

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    insert into fivetran_webhook_events (
                        received_at,
                        event,
                        created_at,
                        connector_id,
                        connector_name,
                        connector_type,
                        sync_id,
                        destination_group_id,
                        status,
                        raw_payload
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """, (
                    datetime.now(timezone.utc),
                    event,
                    created_at,
                    connector_id,
                    connector_name,
                    connector_type,
                    sync_id,
                    destination_group_id,
                    status,
                    json.dumps(payload)
                ))
            conn.commit()

        print(f"Inserted webhook event: event={event} sync_id={sync_id}")
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print(f"Webhook insert failed: {repr(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500