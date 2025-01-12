import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'terry',
    'start_date': dt.datetime(2025, 1, 8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=600),
}


with DAG('coin_scrapper2',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False,
         ) as dag:

    # install_library = BashOperator(task_id='install_library',
    #                            bash_command='python /opt/airflow/dags/extract2.py')
    echo_java = BashOperator(task_id='echo_java',
                               bash_command='sudo -u airflow whoami')
    echo_path = BashOperator(task_id='echo_path',
                               bash_command='sudo -u airflow python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate()"')
    

echo_java >> echo_path
# install_library
