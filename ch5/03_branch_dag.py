import airflow 

import airflow.utils.dates

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

ERP_CHANGE_DATE = airflow.utils.dates.days_ago(1)

def _pick_erp_system(**context) : 
    if context["execution_date"] < ERP_CHANGE_DATE:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"




