from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


dag1 = DAG(dag_id='sql_eg', start_date=datetime(2026,8,24), catchup=False)

query = "create table if not exists employees(id int, name varchar(100));"

query1 = "insert into employees values(1,'jagadish');"

task1 = SQLExecuteQueryOperator(task_id = 'task1',dag=dag1,sql=query,conn_id="airflow_learn")

task2 = SQLExecuteQueryOperator(task_id = 'task2',dag=dag1,sql=query1,conn_id="airflow_learn")

task1 >> task2

