# Airflow with Pyspark inside

- build docker using docker `build -t airflow-spark .`
- run docker compose using `docker compose -f airflow.yaml up`
- when running the airflow, use this to enable pyspark `sudo -u airflow python script.py`