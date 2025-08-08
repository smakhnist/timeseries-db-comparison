import random
import sys
from datetime import datetime, timedelta

import psycopg2
from clickhouse_connect import get_client
from psycopg2.extras import execute_values


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

# ---- Insert into ClickHouse ----
def insert_ticks_clickhouse(client, records):
    client.insert(
        table='trades',
        data=records,                     # list of dicts
        column_names=['timestamp', 'symbol', 'price', 'volume', 'is_buy']
    )

def insert_ticks_psycopg2(con, records):
    with con.cursor() as cur:
        insert_sql = """
                     INSERT INTO trades (timestamp, symbol, price, volume, is_buy) \
                     VALUES %s \
                     """
        execute_values(cur, insert_sql, records)
        con.commit()


def get_client_or_conn(dbtype: str) -> tuple:
    global psycopg2_con, clickhouse_client
    psycopg2_con = None; clickhouse_client = None

    if dbtype == 'clickhouse':
        clickhouse_client = get_client(host='localhost', port=8123, username='default', password='my_secure_password')
    elif dbtype == 'postgresql':
        psycopg2_con = psycopg2.connect(host="localhost",
                                  port=5445,
                                  dbname="mydb",
                                  user="myuser",
                                  password="mypassword")
    elif dbtype == 'questdb':
        psycopg2_con = psycopg2.connect(host="localhost",
                                port=8812,
                                dbname="qdb",
                                user="admin",
                                password="quest")
    elif dbtype == 'timescaledb':
        psycopg2_con = psycopg2.connect(host="localhost",
                                port=5442,
                                dbname="tsdb",
                                user="tsdbuser",
                                password="secretpassword")
    else:
        raise ValueError(f"Unsupported database type: {dbtype}")
    return psycopg2_con, clickhouse_client


def clean_previous_data(psycopg2_con, click_cl):
    if dbtype == 'clickhouse':
        click_cl.command("TRUNCATE TABLE trades")
    elif dbtype in ['postgresql', 'questdb', 'timescaledb']:
        with psycopg2_con.cursor() as cur:
            # Create table if it does not exist
            cur.execute("TRUNCATE TABLE trades")
            psycopg2_con.commit()
    else:
        raise ValueError(f"Unsupported database type: {dbtype}")

def count_records(psycopg2_con, click_cl):
    if dbtype == 'clickhouse':
        return click_cl.query("SELECT COUNT(*) FROM trades").result_rows[0][0]
    elif dbtype in ['postgresql', 'questdb', 'timescaledb']:
        with psycopg2_con.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM trades")
            return cur.fetchone()[0]
    else:
        raise ValueError(f"Unsupported database type: {dbtype}")


# ---- Run ----
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python loaddata.py dbtype") # dbtype is one of: clickhouse, postgresql, questdb, timescaledb")
        sys.exit(1)

    dbtype = sys.argv[1].lower()

    psycopg2_con, click_cl = get_client_or_conn(dbtype)

    clean_previous_data(psycopg2_con, click_cl)

    TRADING_DAYS = 100  # Number of ticks per day
    TICKS_PER_DAY = 100 # trades in one day
    for i in range(TRADING_DAYS):
        start_time = datetime.fromisoformat('2025-01-01') + timedelta(days=i)
        print(f"Generating data for {start_time.strftime('%Y-%m-%d')}")

        # Generate TICKS_PER_DAY ticks for each day
        tick_data = generate_tick_data(TICKS_PER_DAY, start_time)
        # insert the generated data into the database
        if dbtype == 'clickhouse':
            insert_ticks_clickhouse(click_cl, tick_data)
        elif dbtype in ['postgresql', 'questdb', 'timescaledb']:
            insert_ticks_psycopg2(psycopg2_con, tick_data)
        else:
            raise ValueError(f"Unsupported database type: {dbtype}")

    print(f"Inserted {count_records(psycopg2_con, click_cl)} records into {dbtype} database.")