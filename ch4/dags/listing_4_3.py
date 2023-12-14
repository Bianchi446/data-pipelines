import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator 

dag = DAG(
    dag_id = "Chapter4_print_context",
    start_date = airflow.utils.dates.days_ago(3),
    schedule = "@daily",
)

def _print_context(**kargs):
    print(kargs)

print_context = PythonOperator(

    task_id = "print context",
    python_callable = _print_context,
    dag=dag,
)



