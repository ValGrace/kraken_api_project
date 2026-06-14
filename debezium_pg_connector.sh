#!/usr/bin/env bash

curl -i -X PUT -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/krakentrades-connector/config -d '
{

    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": false,
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": false,
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "airflow",
    "database.password": "airflow",
    "database.dbname": "airflow",
    "topic.prefix": "krakentopic",
    "schema.include.list": "public",
    "table.include.list": "public.assetpairs,public.market_depth,public.ohlc_sticks,public.recent_trades, public.bin_candles"
    
}'

echo "Debezium connector successfully registered"

curl -i -X GET -H "Accept:application/json" localhost:8083/connectors/krakentrades-connector/status