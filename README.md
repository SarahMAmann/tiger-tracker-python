To create a python project where you can run this code:

```python
cd tiger-crypto-tracker
python3 -m venv .venv
source .venv/bin/activate
pip install psycopg2-binary requests python-dotenv
```

SQL query to create tables:

```sql
-- Create users table
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name TEXT
);

-- Create assets table
CREATE TABLE IF NOT EXISTS assets (
  id SERIAL PRIMARY KEY,
  symbol TEXT,
  name TEXT
);

-- Drop and recreate transactions table properly
DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions (
  id SERIAL,
  user_id INT REFERENCES users(id),
  asset_id INT REFERENCES assets(id),
  amount NUMERIC,
  price_usd NUMERIC,
  ts TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (id, ts) -- ✅ valid composite key for Timescale
);

-- Convert to hypertable — safely
SELECT create_hypertable('transactions', 'ts', if_not_exists => TRUE);
```
