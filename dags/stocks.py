import os
import pandas as pd
import yfinance as yf

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def get_tickers():
    b3_tickers = [
        "PETR4.SA",
        "VALE3.SA",
        "ITUB4.SA",
        "ABEV3.SA",
        "WEGE3.SA",
        "MGLU3.SA", 
    ]

    us_tickers = [
        "AAPL",  
        "MSFT",   
        "NVDA", 
        "TSLA",   
        "JNJ",    
        "ABNB",   
    ]

    crypto_tickers = [
        "BTC-USD",   
        "ETH-USD",  
        "SOL-USD",
        "ADA-USD",
        "DOGE-USD",
    ]

    return b3_tickers + us_tickers + crypto_tickers


def fetch_ticker_data(ticker):
    try:
        df = yf.Ticker(ticker).history(period="7d", interval="1h")

        if df.empty:
            print(f"Sem dados para {ticker}")
            return pd.DataFrame()

        df["Ticker"] = ticker
        df.reset_index(inplace=True)
        df.columns = [col.lower() for col in df.columns]

        return df

    except Exception as e:
        print(f"Erro ao buscar {ticker}: {e}")
        return pd.DataFrame()


def create_quotes_dataframe():
    tickers = get_tickers()
    data_list = []

    for ticker in tickers:
        df = fetch_ticker_data(ticker)

        if not df.empty:
            data_list.append(df)

    if not data_list:
        return pd.DataFrame()

    return pd.concat(data_list, ignore_index=True)


def transform_quotes(df):
    if df.empty:
        return df

    df = df.rename(
        columns={
            "stock splits": "stock_splits",
            "capital gains": "capital_gains",
        }
    )

    return df


def get_engine():
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )


def create_stock_quotes_table(engine):
    ddl = """
        CREATE TABLE IF NOT EXISTS stock_quotes (
            datetime TIMESTAMPTZ,
            open FLOAT(53),
            high FLOAT(53),
            low FLOAT(53),
            close FLOAT(53),
            volume BIGINT,
            dividends FLOAT(53),
            stock_splits FLOAT(53),
            ticker VARCHAR,
            capital_gains FLOAT(53),
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
            stock_splits = EXCLUDED.stock_splits,
            capital_gains = EXCLUDED.capital_gains
    """

    conn.execute(text(sql), data)


def load_quotes(df, engine):
    if df.empty:
        print("Nenhum dado para carregar.")
        return

    df.to_sql(
        "stock_quotes",
        con=engine,
        if_exists="append",
        index=False,
        method=postgres_upsert,
    )


def run_pipeline_stocks():
    engine = get_engine()

    raw_df = create_quotes_dataframe()
    transformed_df = transform_quotes(raw_df)

    create_stock_quotes_table(engine)
    load_quotes(transformed_df, engine)


if __name__ == "__main__":
    run_pipeline_stocks()