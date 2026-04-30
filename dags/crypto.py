import os
import time
import math
import requests
import pandas as pd

from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def get_engine():
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )


def get_crypto_tickers():
    return [
        "bitcoin",
        "ethereum",
        "solana",
    ]


def fetch_crypto_quotes():
    crypto_tickers = get_crypto_tickers()
    chunk_size = 30
    total_chunks = math.ceil(len(crypto_tickers) / chunk_size)

    crypto_data = []

    for i in range(total_chunks):
        start = i * chunk_size
        end = start + chunk_size

        cryptos = crypto_tickers[start:end]
        ids = ",".join(cryptos)

        url = (
            "https://api.coingecko.com/api/v3/simple/price"
            f"?ids={ids}"
            "&vs_currencies=usd"
            "&include_market_cap=true"
            "&include_24hr_vol=true"
        )

        try:
            response = requests.get(url, timeout=10)

            if response.status_code != 200:
                print(f"Erro HTTP {response.status_code}: {response.text}")
                continue

            data = response.json()

            collected_at = datetime.now(timezone.utc).replace(
                minute=0,
                second=0,
                microsecond=0,
            )

            for coin_id, values in data.items():
                crypto_data.append(
                    {
                        "coin_id": coin_id,
                        "datetime": collected_at,
                        "price_usd": values.get("usd"),
                        "market_cap_usd": values.get("usd_market_cap"),
                        "vol_24h_usd": values.get("usd_24h_vol"),
                    }
                )

        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")

        except ValueError as e:
            print(f"Erro ao converter resposta para JSON: {e}")

        finally:
            time.sleep(2)

    return pd.DataFrame(crypto_data)


def create_crypto_quotes_table(engine):
    ddl = """
        CREATE TABLE IF NOT EXISTS crypto_quotes (
            coin_id VARCHAR,
            datetime TIMESTAMPTZ,
            price_usd FLOAT(53),
            market_cap_usd FLOAT(53),
            vol_24h_usd FLOAT(53),
            PRIMARY KEY (coin_id, datetime)
        );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))


def postgres_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]

    if not data:
        return

    sql = f"""
        INSERT INTO {table.table.name} ({', '.join(keys)})
        VALUES ({', '.join([f":{k}" for k in keys])})
        ON CONFLICT (coin_id, datetime)
        DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            market_cap_usd = EXCLUDED.market_cap_usd,
            vol_24h_usd = EXCLUDED.vol_24h_usd
    """

    conn.execute(text(sql), data)


def load_crypto_quotes(df, engine):
    if df.empty:
        print("Nenhum dado coletado. Encerrando pipeline.")
        return

    df.to_sql(
        "crypto_quotes",
        con=engine,
        if_exists="append",
        index=False,
        method=postgres_upsert,
    )

    print(f"{len(df)} registros carregados na tabela crypto_quotes.")


def run_pipeline_crypto():
    engine = get_engine()

    df = fetch_crypto_quotes()

    create_crypto_quotes_table(engine)

    load_crypto_quotes(df, engine)


if __name__ == "__main__":
    run_pipeline_crypto()