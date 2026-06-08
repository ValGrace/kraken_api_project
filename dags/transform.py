from extract import extract_asset_pairs
from datetime import datetime
from airflow.sdk import asset, dag
import json


@asset(schedule=extract_asset_pairs)
def transform_asset_pairs(context):    
    asset_pair_data = context["ti"].xcom_pull(
        dag_id="extract_asset_pairs",
        task_ids="extract_asset_pairs",
        key="return_value"
    )
    
    # Defensive check: Ensure we actually got data
    if not asset_pair_data:
        raise ValueError("extract_asset_pairs returned no data.")

    asset_pairs = []
    for data in asset_pair_data:
        asset_pairs.append({
            "altname": data.get("altname"),
            "base": data.get("base"),
            "aclass_base": data.get("aclass_base"),
            "lot": data.get("lot"),
            "cost_decimals": data.get("cost_decimals"),
            "leverage_buy": data.get("leverage_buy", []),
            "leverage_sell": data.get("leverage_sell", []),
            "inserted_at": datetime.now()
        })
    
    return asset_pairs



# transform_asset_pairs()

# @asset(schedule=[extract_market_depth])
# def transform_market_depth(context: dict) -> list:
#     market_depth = []

#     market_depth_data = context["ti"].xcom_pull(
#         dag_id="extract_market_depth",
#         task_ids=["extract_market_depth"],
#         key="return_value",
#         include_prior_dates=True
#     ) 

#     for data in market_depth_data.get("result"):
#         market_depth.append({
#             "btc_asks": data.get("BTC/USD", {}).get("asks", []),
#             "btc_bids": data.get("BTC/USD", {}).get("bids", [])
#         })

#     return market_depth

# @asset(schedule=[extract_ohlc_data])
# def transform_ohlc_data(context: dict) -> list:
#     ohlc_sticks = []

#     ohlc_data = context["ti"].xcom_pull(
#         dag_id="extract_ohlc_data",
#         task_ids=["extract_ohlc_data"],
#         key="return_value",
#         include_prior_dates=True

#     )
#     for data in ohlc_data.get("result").get("BTC/USD"):
#         ohlc_sticks.append({
#             "btc_asks": data
#         })

#     return ohlc_sticks

# @asset(schedule=[extract_recent_trades])
# def transform_recent_trades(context: dict) -> list:
#     recent_trades = []

#     recent_trades_data = context["ti"].xcom_pull(
#         dag_id="extract_recent_trades",
#         task_ids=["extract_recent_trades"],
#         key="return_value",
#         include_prior_dates=True
#     )
   
#     recent_trades.append({
#         "trade": recent_trades_data.get("result").get("BTC/USD")
#     })

#     return recent_trades