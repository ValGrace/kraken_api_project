import os
from airflow.sdk import asset
import pandas as pd
from sqlalchemy import create_engine
from extract import extractasset

@asset(
    schedule=[extractasset]
)
def store_asset_pairs(**context):
    """
    Format the asset pair data
    """
    pair_data = context["ti"].xcom_pull(
        dag_id="extractasset",
        task_ids=["extractasset"],
        key="new_asset_value",
        include_prior_dates=True
    )

    # _df = pd.DataFrame(pair_data)
    return pair_data
    # engine = create_engine('postgres://airflow@airflow/airflow')
    # return _df.to_sql("asset_pairs", con=engine, if_exists='append', index=False)