from krakrequests import request_sect
from airflow.sdk import asset
from datetime import timedelta
import json
import pandas as pd
from sqlalchemy import create_engine

@asset(schedule=timedelta(minutes=5))
def bin_candles():
    res = request_sect(
        method="GET",
        path="/api/v3/klines",
        query={
            "symbol": "BTCUSDT",
            "interval":"1h",
            "limit":60
        },
        environment="https://api.binance.com"
    )

    data = res.read().decode()

    candlesticks = json.loads(data)

    candles = []

    for stick in candlesticks:
        (
        timestamp, open_p, high, low, close, volume, close_time, quote_volume, trades, taker_base, taker_quote, ignore
        ) = stick
        

        candles.append({
            "timestamp": timestamp,
            "open_p": float(open_p),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume),
            "close_time": close_time,
            "quote_volume": float(quote_volume),
            "trades": trades,
            "taker_base": float(taker_base),
            "taker_quote": float(taker_quote),
            "ignore": ignore
        })

        df_bin_candles = pd.Dataframe(candles)
        engine = create_engine("postgresql://airflow:airflow@postgres/airflow")

        df_bin_candles.to_sql("bin_candles", con=engine, if_exists='append', index=False)
