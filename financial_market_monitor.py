import pandas as pd
import yfinance as yf
import datetime

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

ativo = yf.Ticker('PETR4.SA')

dados = ativo.history(period="1d", interval="1h")
dados.rename(columns={'Stock Splits': 'Stock_Splits'}, inplace=True)
dados.reset_index(inplace=True)
dados['Ticker'] = 'PETR4.SA'
dados.columns = [col.lower() for col in dados.columns]

# Formato da URL: dialeto://usuario:senha@host:porta/banco
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:5432/{DB_NAME}")

ddl = """
    DROP TABLE IF EXISTS stock_quotes;

    CREATE TABLE stock_quotes (
        datetime TIMESTAMPTZ, 
        open FLOAT(53), 
        high FLOAT(53), 
        low FLOAT(53), 
        close FLOAT(53), 
        volume BIGINT, 
        dividends FLOAT(53), 
        stock_splits FLOAT(53),
        ticker VARCHAR,
        PRIMARY KEY (ticker, datetime)

);

"""


with engine.begin() as conn:
    conn.execute(text(ddl))


def postgres_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    
    sql = f"""
        INSERT INTO {table.table.name} ({', '.join(keys)})
        VALUES ({', '.join([f":{k}" for k in keys])})
        ON CONFLICT (ticker, datetime)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            dividends = EXCLUDED.dividends,
            stock_splits = EXCLUDED.stock_splits
    """
    conn.execute(text(sql), data)

dados.to_sql('stock_quotes', engine, if_exists='append', index=False, method=postgres_upsert)