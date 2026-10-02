from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
from helper import postgres_data as pd

with DAG(
    dag_id="my_dag",
    schedule_interval='@once',
    #is_paused_upon_creation=False,
    default_args = {
        "owner": "Ruslan",
        "start_date": datetime(2026,3,2),
        "catchup": True,
        "retries": 3,
        "retry_delay": timedelta(seconds=30)
        #"depends_on_past": True
    },
    tags = ["RealTime"]

) as mydag:
    
    debezium_sensor = HttpSensor(
        task_id='debezium_sensor',
        http_conn_id='debezium_connection',
        endpoint='/connectors',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        poke_interval=10,
        timeout=120,
        mode='reschedule'
    )

    postgres_sensor = SqlSensor(
        task_id='postgres_sensor',
        conn_id='postgres_connection',
        sql='select 1;',
        poke_interval=10,
        timeout=120,
        mode='reschedule'
    )

    debezium_curl = BashOperator(
        task_id='cdc_curl',
        bash_command='curl -s -X POST http://kafka-connect:8083/connectors -H "Content-Type: application/json" -d @/opt/airflow/dags/helper/debezium_connector/pipeline.json | python3 -m json.tool'
    )

    db_crud_task = PythonOperator(
        task_id="execute_db_crud_operations",
        python_callable=pd.execute_postgres_crud,
        )

    [debezium_sensor, postgres_sensor] >> debezium_curl >> db_crud_task
    
