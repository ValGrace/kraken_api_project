from krakrequests import request_sect
from airflow.sdk import asset
import json

@asset(schedule="@daily")
def extract_asset_pairs():
    response = request_sect(
        method="GET",
        path="/0/public/AssetPairs",
        environment="https://api.kraken.com"
    )
    print(response.status)

    data = response.read().decode()
    data = json.loads(data)
    return data.get("result")

# @asset(schedule="@daily")
# def extract_recent_trades():
#     response = request_sect(
#         method="GET",
#         path="/0/public/Trades",
#         query={
#             "pair": "BTC/USD",
#             "count": 2,
#         },
#         environment="https://api.kraken.com",
#     )
#     print(response.status)
#     data = response.read().decode()
#     return data
    
# @asset(schedule="@daily")
# def extract_ohlc_data():
#     response = request_sect(
#         method="GET",
#         path="/0/public/OHLC",
#         query={
#             "pair": "BTC/USD",
#             "interval": 5,
#         },
#         environment="https://api.kraken.com",
#     )
#     print(response.status)

#     data = response.read().decode()

#     return data

# @asset(schedule="@daily")
# def extract_market_depth():
#     response = request_sect(
#         method="GET",
#         path="/0/public/Depth",
#         query={
#             "pair": "BTC/USD",
#             "count": 10
#         },
#         environment="https://api.kraken.com",
#     )
#     print(response.status)
#     data = response.read().decode()

#     return data



