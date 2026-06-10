curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '
{
    "name": "krakentrades-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
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
        "topic.prefix": "krakentrades",
        "schema.include.list": "public",
        "table.include.list": "public.assetpairs,public.market_depth,public.ohlc_sticks,public.recent_trades"
    }
}'

echo "Debezium connector successfully registered"

curl -i -X GET -H "Accept:application/json" localhost:8083/connectors/krakentrades-connector/status