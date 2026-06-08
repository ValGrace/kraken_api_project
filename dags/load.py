import os
from airflow.sdk import asset
import pandas as pd
from sqlalchemy import create_engine
from transform import transform_asset_pairs

@asset(
    schedule=[transform_asset_pairs]
)
def store_asset_pairs(context: dict):
    """
    Format the asset pair data
    """
    pair_data = context["ti"].xcom_pull(
        dag_id="transform_asset_pairs",
        task_ids=["transform_asset_pairs"],
        key="return_value",
        include_prior_dates=True
    )

    _df = pd.DataFrame(pair_data)
    engine = create_engine('postgres://airflow@airflow/airflow')
    return _df.to_sql("asset_pairs", con=engine, if_exists='append', index=False)