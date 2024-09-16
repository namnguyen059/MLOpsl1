from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Define the path to the scripts inside the Docker container
TRAIN_SCRIPT = '/opt/airflow/components/train.py'
PREDICT_SCRIPT = '/opt/airflow/components/predict.py'
EVALUATE_SCRIPT = '/opt/airflow/components/evaluate.py'

def train_model():
    os.system(f'python {TRAIN_SCRIPT}')

def predict_model():
    os.system(f'python {PREDICT_SCRIPT}')

def evaluate_model():
    os.system(f'python {EVALUATE_SCRIPT}')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(dag_id='house_price_dag', default_args=default_args, schedule_interval=None) as dag:
    
    train_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )
    
    predict_task = PythonOperator(
        task_id='predict_model',
        python_callable=predict_model
    )
    
    evaluate_task = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model
    )
    
    train_task >> predict_task >> evaluate_task
