import os
import time
import datetime as dt
import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Load .env (from the same folder as this script)
load_dotenv()
PGURL = os.getenv("TIMESCALE_SERVICE_URL")
if not PGURL:
    raise RuntimeError("TIMESCALE_SERVICE_URL not found in .env")

# CoinGecko asset ids → (symbol, name)
ASSETS = {
    "bitcoin": ("BTC", "Bitcoin"),
    "ethereum": ("ETH", "Ethereum"),
    # add more if you like:
    # "solana": ("SOL", "Solana"),
    # "arbitrum": ("ARB", "Arbitrum"),
}

POLL_SECONDS = 30  # how often to ingest


def ensure_schema(conn):
    """Create base tables if they don't exist and make transactions a hypertable."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            id SERIAL PRIMARY KEY,
            symbol TEXT,
            name TEXT
        );
        """)
        # transactions with composite PK so Timescale can hypertable it
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL,
            user_id INT REFERENCES users(id),
            asset_id INT REFERENCES assets(id),
            amount NUMERIC,
            price_usd NUMERIC,
            ts TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (id, ts)
        );
        """)
        # Make it a hypertable (no error if it already is)
        cur.execute("SELECT create_hypertable('transactions', 'ts', if_not_exists => TRUE);")

        # Ensure a uniqueness constraint or index on assets(symbol) so we can upsert
        cur.execute("""
            SELECT 1
            FROM pg_indexes
            WHERE tablename = 'assets' AND indexname = 'assets_symbol_key';
        """)
        if not cur.fetchone():
            cur.execute("CREATE UNIQUE INDEX assets_symbol_key ON assets(symbol);")

    conn.commit()


def seed_reference_data(conn):
    """Insert a demo user and asset rows (idempotent)."""
    with conn.cursor() as cur:
        # demo user
        cur.execute("INSERT INTO users (name) VALUES ('Sarah') ON CONFLICT DO NOTHING;")

        # upsert assets (requires unique index on symbol)
        for _, (symbol, name) in ASSETS.items():
            cur.execute("""
                INSERT INTO assets (symbol, name)
                VALUES (%s, %s)
                ON CONFLICT (symbol) DO NOTHING;
            """, (symbol, name))
    conn.commit()


def get_asset_id_map(conn):
    """Return {SYMBOL: asset_id}."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, symbol FROM assets;")
        rows = cur.fetchall()
    return {sym: aid for aid, sym in rows}


def fetch_prices():
    """Fetch current USD prices from CoinGecko for our ASSETS."""
    ids = ",".join(ASSETS.keys())
    url = "https://api.coingecko.com/api/v3/simple/price"
    r = requests.get(url, params={"ids": ids, "vs_currencies": "usd"}, timeout=15)
    r.raise_for_status()
    return r.json()  # e.g., {"bitcoin": {"usd": 67000.0}, ...}


def ingest_once(conn):
    """Fetch prices and insert a row per asset into transactions."""
    data = fetch_prices()
    ts = dt.datetime.utcnow()

    # Map symbol -> id
    asset_id_by_symbol = get_asset_id_map(conn)

    rows = []
    for cg_id, payload in data.items():
        symbol = ASSETS[cg_id][0]  # e.g., "BTC"
        asset_id = asset_id_by_symbol[symbol]
        price = payload["usd"]
        # Store amount=1 to make “value” == price (you can extend this later with real holdings)
        rows.append((1, asset_id, 1, price, ts))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO transactions (user_id, asset_id, amount, price_usd, ts)
            VALUES %s
            """,
            rows
        )
    conn.commit()
    print(f"[{ts.isoformat()}Z] inserted {len(rows)} rows")


def main():
    # Use keepalives to be resilient during long runs
    conn = psycopg2.connect(PGURL, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)
    try:
        ensure_schema(conn)
        seed_reference_data(conn)

        print("Starting ingestion loop. Press Ctrl+C to stop.")
        while True:
            try:
                ingest_once(conn)
            except requests.HTTPError as e:
                print(f"HTTP error from API: {e}. Retrying after short delay…")
            except Exception as e:
                # If anything else fails, rollback this cycle but keep running
                print(f"Ingest error: {e}. Rolling back this cycle.")
                conn.rollback()
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        print("Stopping.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
