import random
from datetime import datetime, timedelta
from clickhouse_connect import get_client

SYMBOLS = ["AAPL", "MSFT", "GOOG"]

# ---- Generate Synthetic Tick Data ----
def generate_tick_data(n:int, start_time:datetime):
    data = []
    base_time = start_time

    for i in range(n):
        si = random.randrange(len(SYMBOLS))
        symbol = SYMBOLS[si]
        price = round(random.uniform((si+1)*100, (si+2)*100), 2)
        volume = random.randint(1, 1000)
        is_buy = random.choice([True, False])
        tick_time = base_time + timedelta(milliseconds=i * (86400000 / n))  # spread ticks by 1ms
        data.append((tick_time, symbol, price, volume, is_buy))

    return data

# ---- Insert into TimescaleDB ----
def insert_ticks(client, records):
    client.insert(
        table='trades',
        data=records,                     # list of dicts
        column_names=['timestamp', 'symbol', 'price', 'volume', 'is_buy']
    )

# ---- Run ----
if __name__ == "__main__":
    client = get_client(host='localhost', port=8123, username='default', password='my_secure_password')

    TRADING_DAYS = 100  # Number of ticks per day
    TICKS_PER_DAY = 100000 # trades in one day
    for i in range(TRADING_DAYS):
        start_time = datetime.fromisoformat('2025-01-01') + timedelta(days=i)
        print(f"Generating data for {start_time.strftime('%Y-%m-%d')}")

        # Generate TICKS_PER_DAY ticks for each day
        tick_data = generate_tick_data(TICKS_PER_DAY, start_time)
        # insert the generated data into the database
        insert_ticks(client, tick_data)
