from kafka import KafkaProducer
import json
import pandas as pd
from sqlalchemy import create_engine

conn_string = "postgresql://airflow:airflow@postgres/airflow"
engine = create_engine(conn_string)

kraken_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    api_version=(2, 3, 1),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'kraken_trades_topic'


