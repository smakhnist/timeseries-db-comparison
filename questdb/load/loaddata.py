import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

# ---- Configuration ----
DB_CONFIG = {
    "host": "localhost",
    "port": 8812,
    "dbname": "qdb",
    "user": "admin",
    "password": ""
}

SYMBOLS = ["AAPL", "MSFT", "GOOG"]

# ---- Generate Synthetic Tick Data ----
def generate_tick_data(n:int, start_time:datetime):
    data = []
    base_time = start_time

    for i in range(n):
        si = random.randrange(len(SYMBOLS))
        symbol = SYMBOLS[si]
        price = round(random.uniform((si+1)*100, (si+2)*100), 2) # price between 100 and 300
        volume = random.randint(1, 1000) # volume between 1 and 1000
        is_buy = random.choice([True, False]) # randomly choose buy or sell
        tick_time = base_time + timedelta(milliseconds=i * (86400000 / n))  # spread ticks by 1ms
        data.append((tick_time, symbol, price, volume, is_buy))

    return data

# ---- Insert into TimescaleDB ----
def insert_ticks(con, records):
    with con.cursor() as cur:
        insert_sql = """
                     INSERT INTO trades (timestamp, symbol, price, volume, is_buy) \
                     VALUES %s \
                     """
        execute_values(cur, insert_sql, records)
        con.commit()


# ---- Run ----
if __name__ == "__main__":
    conn = psycopg2.connect(host="localhost",
                            port=8812,
                            dbname="qdb",
                            user="admin",         # required even if not checked
                            password="quest")
    TRADING_DAYS = 100  # Number of ticks per day
    TICKS_PER_DAY = 100000 # trades in one day
    for i in range(TRADING_DAYS):
        start_time = datetime.fromisoformat('2025-01-01') + timedelta(days=i)
        print(f"Generating data for {start_time.strftime('%Y-%m-%d')}")

        # Generate TICKS_PER_DAY ticks for each day
        tick_data = generate_tick_data(TICKS_PER_DAY, start_time)
        # insert the generated data into the database
        insert_ticks(conn, tick_data)
    conn.close()