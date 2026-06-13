### Kraken Real time streaming architecture
![Architecture]("https://github.com/ValGrace/kraken_api_project/blob/main/assets/rtal.png")


Create a virtual environment

```bash
python -m venv krakenvenv
```

Install required packages
```bash
pip install requirements.txt
```

Make the shell script executable
```bash
chmod +x flowinit.sh
```

Initialize the database and create admin user
```bash
docker compose up airflow-init
```

start the container services
```bash
docker compose up
```

Open the Airflow UI through `http://localhost:8080`
Use the credentials:-
`Username: airflow`
`Password: airflow`

Execute postgres source connector configuration
```bash
chmod +x debezium_pg_connector.sh

bash debezium_pg_connector.sh
```

Run

