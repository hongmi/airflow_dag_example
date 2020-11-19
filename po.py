import sys
import abc
import csv
import random
import string
import bson
import datetime

from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
import pprint
import copy
from multiprocessing import Process, Value, Array
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'python_operator',
    default_args=default_args,
    description='a python operator dag',
    schedule_interval=datetime.timedelta(days=1),
)

def log_timestamp(s):
    print(datetime.datetime.now().time(), s)

def task_func():
#if __name__ == '__main__':
    print(sys.version)
    print(sys.path)
    print("hhheee")

run_this = PythonOperator(
    task_id="python_operator_aa",
    provide_context=True,
    python_callable=task_func,
    dag=dag
)

run_this
