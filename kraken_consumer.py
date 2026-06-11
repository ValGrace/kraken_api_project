from kafka import KafkaConsumer
import json
import pandas as pd
from sqlalchemy import create_engine

conn_string = "postgresql://airflow:airflow@postgres/airflow"
engine = create_engine(conn_string)
def kraken_consumer():
    kraken_trades_consumer = KafkaConsumer(
        'krakentrades',
        bootstrap_servers='broker:9092',
        enable_auto_commit=True,
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(m.decode('utf-8'))
    )





