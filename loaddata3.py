import random
import sys
import time
from datetime import datetime, timedelta
import psycopg
import socket
from clickhouse_connect import get_client


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

def insert_ticks_questdb(records):
    BUFFER_STEP = 100000
    for i in range(0, len(records), BUFFER_STEP):
        questdb_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        questdb_sock.connect(("localhost", 9009))

        batch = records[i:i + BUFFER_STEP]
        lines = []
        for record in batch:
            lines.append(f"trades,symbol={record[1]} price={record[2]},volume={record[3]}i,is_buy={record[4]} {int(record[0].timestamp() * 1e9)}")
        payload = "\n".join(lines) + "\n"
        questdb_sock.sendall(payload.encode("utf-8"))
        time.sleep(0.01)
        questdb_sock.close()

def insert_ticks_psycopg(con, records):
    with con.cursor() as cur:
        insert_sql = """
                     INSERT INTO trades (timestamp, symbol, price, volume, is_buy) \
                     VALUES (%s, %s, %s, %s, %s) \
                     """
        cur.executemany(insert_sql, records)
        con.commit()


def get_client_or_conn(dbtype: str) -> tuple:
    global psycopg_con, clickhouse_client, questdb_sock
    psycopg_con = None; clickhouse_client = None; questdb_sock = None

    if dbtype == 'clickhouse':
        clickhouse_client = get_client(host='localhost', port=8123, username='default', password='my_secure_password', compress=True)
    elif dbtype == 'postgresql':
        psycopg_con = psycopg.connect(host="localhost",
                                  port=5445,
                                  dbname="mydb",
                                  user="myuser",
                                  password="mypassword")
    elif dbtype == 'questdb':
        psycopg_con = psycopg.connect(host="localhost",
                                      port=8812,
                                      dbname="qdb",
                                      user="admin",
                                      password="quest")
    elif dbtype == 'timescaledb':
        psycopg_con = psycopg.connect(host="localhost",
                                port=5442,
                                dbname="tsdb",
                                user="tsdbuser",
                                password="secretpassword")
    else:
        raise ValueError(f"Unsupported database type: {dbtype}")
    return psycopg_con, clickhouse_client


def clean_previous_data(psycopg_con, click_cl):
    if dbtype == 'clickhouse':
        click_cl.command("TRUNCATE TABLE trades")
    elif dbtype in ['postgresql', 'timescaledb']:
        with psycopg_con.cursor() as cur:
            # Create table if it does not exist
            cur.execute("TRUNCATE TABLE trades")
            psycopg_con.commit()
    elif dbtype       == 'questdb':
        with psycopg_con.cursor() as cur:
            # Create table if it does not exist
            cur.execute("DROP TABLE IF EXISTS trades;")
            cur.execute("""
                        CREATE TABLE trades (
                                                timestamp TIMESTAMP,
                                                symbol SYMBOL,           -- indexed automatically
                                                price DOUBLE,
                                                volume INT,
                                                is_buy BOOLEAN
                        ) timestamp(timestamp);
                        """)
            psycopg_con.commit()
    else:
        raise ValueError(f"Unsupported database type: {dbtype}")

def count_records(psycopg_con, click_cl):
    if dbtype == 'clickhouse':
        return click_cl.query("SELECT COUNT(*) FROM trades").result_rows[0][0]
    elif dbtype in ['postgresql', 'questdb', 'timescaledb']:
        with psycopg_con.cursor() as cur:
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

    psycopg_con, click_cl = get_client_or_conn(dbtype)

    clean_previous_data(psycopg_con, click_cl)

    TRADING_DAYS = 100  # Number of ticks per day
    TICKS_PER_DAY = 1000000 # trades in one day
    for i in range(TRADING_DAYS):
        start_time = datetime.fromisoformat('2025-01-01') + timedelta(days=i)
        print(f"Generating data for {start_time.strftime('%Y-%m-%d')}")

        # Generate TICKS_PER_DAY ticks for each day
        tick_data = generate_tick_data(TICKS_PER_DAY, start_time)
        # insert the generated data into the database
        if dbtype == 'clickhouse':
            insert_ticks_clickhouse(click_cl, tick_data)
        elif dbtype  == 'questdb':
            insert_ticks_questdb(tick_data)
        elif dbtype in ['postgresql', 'timescaledb']:
            insert_ticks_psycopg(psycopg_con, tick_data)
        else:
            raise ValueError(f"Unsupported database type: {dbtype}")

    print(f"Inserted {count_records(psycopg_con, click_cl)} records into {dbtype} database.")