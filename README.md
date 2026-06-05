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

