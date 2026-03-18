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
            cur.execute("""
                create index if not exists idx_fwe_event
                on fivetran_webhook_events (event);
            """)
            cur.execute("""
                create index if not exists idx_fwe_sync_id
                on fivetran_webhook_events (sync_id);
            """)
            cur.execute("""
                create index if not exists idx_fwe_created_at
                on fivetran_webhook_events (created_at);
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

@app.route("/fivetran/webhook", methods=["POST", "OPTIONS"])
@app.route("/fivetran/webhook/", methods=["POST", "OPTIONS"])
def fivetran_webhook():
    if request.method == "OPTIONS":
        return "", 200

    payload = request.get_json(silent=True) or {}

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

    print("\n" + "=" * 80)
    print(f"received_at: {datetime.now(timezone.utc).isoformat()}")
    print(f"event: {event}")
    print(f"connector_id: {connector_id}")
    print(f"connector_name: {connector_name}")
    print(f"sync_id: {sync_id}")
    print(f"created: {payload.get('created')}")
    print(f"data: {json.dumps(payload.get('data', {}), indent=2)}")
    print("=" * 80 + "\n")

    return jsonify({"status": "ok"}), 200

init_db()