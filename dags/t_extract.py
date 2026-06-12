from airflow.sdk import asset
from krakrequests import request_sect
from sqlalchemy import create_engine
from datetime import timedelta
import json
import pandas as pd

@asset(schedule=timedelta(minutes=5))
def extract_recent_trades():
    response = request_sect(
        method="GET",
        path="/0/public/Trades",
        query={
            "pair": "BTC/USD",
            "count": 1,
        },
        environment="https://api.kraken.com",
    )
    print(response.status)
    data = response.read().decode()
    data = json.loads(data)
    trades = data["result"].get("BTC/USD", [])
    recent_trades = []
    for trade in trades:
        price, volume, time, side, order_type, misc, trade_id = trade
        recent_trades.append({
            "price": price,
            "ask_volume": volume,
            "timestamp": time,
            "side": side,
            "order": order_type,
            "misc": misc,
            "trade_id": trade_id
        })
    
    df_recent_trades = pd.DataFrame(recent_trades)
    engine = create_engine("postgresql://airflow:airflow@postgres/airflow")
    df_recent_trades.to_sql("recent_trades", con=engine, if_exists='append', index=False)
    return recent_trades
    
@asset(schedule=timedelta(minutes=5))
def extract_ohlc_data():
    response = request_sect(
        method="GET",
        path="/0/public/OHLC",
        query={
            "pair": "BTC/USD",
            "interval": 5,
        },
        environment="https://api.kraken.com",
    )
    print(response.status)

    ohlc_data = response.read().decode()
    data = json.loads(ohlc_data)
    result = data.get("result")
    ohlc_sticks = []

    for data in result.get("BTC/USD"):
            ohlc_sticks.append({
                "btc_asks_ts": data[0],
                "btc_asks_open": data[1],
                "btc_asks_high": data[2],
                "btc_asks_low": data[3],
                "btc_asks_close": data[4],
                "btc_asks_vwap": data[5],
                "btc_asks_volume": data[6],
                "btc_asks_trades": data[7]
            })
    df_ohlc_sticks = pd.DataFrame(ohlc_sticks)
    engine = create_engine("postgresql://airflow:airflow@postgres/airflow")
    df_ohlc_sticks.to_sql("ohlc_sticks", con=engine, if_exists='append', index=False)
    return ohlc_sticks

@asset(schedule=timedelta(seconds=5))
def extract_market_depth():
    response = request_sect(
        method="GET",
        path="/0/public/Depth",
        query={
            "pair": "BTC/USD",
            "count": 1
        },
        environment="https://api.kraken.com",
    )
    print(response.status)
    market_depth_data = response.read().decode()
    print(market_depth_data)
    data = json.loads(market_depth_data)
    result = data.get("result")
    print(result)
    market_depth = []

    for coin, mdata in result.items():
        market_depth.append({
            "ask_price": mdata.get("asks", [])[0][0],
            "ask_volume": mdata.get("asks", [])[0][1],
            "ask_ts": mdata.get("asks", [])[0][2],
            "bid_price": mdata.get("bids", [])[0][0],
            "bid_volume": mdata.get("bids", [])[0][1],
            "bid_ts": mdata.get("bids", [])[0][2],
        })

    df_market_depth = pd.DataFrame(market_depth)
    engine = create_engine("postgresql://airflow:airflow@postgres/airflow")
    df_market_depth.to_sql("market_depth", con=engine, if_exists='append', index=False)   
    return market_depth
