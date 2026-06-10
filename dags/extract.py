from krakrequests import request_sect
from airflow.sdk import asset, dag
from airflow.decorators import task
from sqlalchemy import create_engine
import pandas as pd
import logging
import json
from datetime import timedelta

@asset(schedule=timedelta(minutes=2))
def extractasset(context):
    response = request_sect(
        method="GET",
        path="/0/public/AssetPairs",
        environment="https://api.kraken.com"
    )
    print(response.status)

    data = response.read().decode()
    data = json.loads(data)
    result = data.get("result")
    asset_pairs = []
    print(result)
    for altname, data in result.items():
        asset_pairs.append({
            "altname": altname,
            "base": data.get("base"),
            "aclass_base": data.get("aclass_base"),
            "lot": data.get("lot"),
            "cost_decimals": data.get("cost_decimals"),
            "leverage_buy": data.get("leverage_buy", []),
            "leverage_sell": data.get("leverage_sell", [])
        })
    pairs = context["ti"].xcom_push(key="new_asset_value", value=asset_pairs)
    engine = create_engine("postgresql://airflow:airflow@postgres/airflow")
    _df = pd.DataFrame(pairs)
    _df.to_sql("assetpairs", con=engine, if_exists='append', index=False)
    return pairs

@asset(schedule=extractasset)
def access_assetpairs(context):
    pairs = context['task_instance'].xcom_pull(dag_id="extractassets",key="new_asset_value", task_ids="extractassets")
    
    print("pairs: ", pairs)
    print(context["task_instance"].xcom_pull(dag_id="extractassets"))

    return pairs
