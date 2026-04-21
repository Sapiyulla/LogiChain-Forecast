from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore

with DAG(
    dag_id='minimal_test_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',  # раз в день
    catchup=False,
) as dag:
    
    hello = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello, Airflow!"'
    )

    end = BashOperator(
        task_id='say_end',
        bash_command='echo "Hello, Airflow!"'
    )

    hello >> end