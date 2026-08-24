from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


dag1 = DAG(dag_id = "first_bash_dag",start_date=datetime(2026,8,24), catchup=False)

task1 = BashOperator(task_id='task1',bash_command='date',dag=dag1)



