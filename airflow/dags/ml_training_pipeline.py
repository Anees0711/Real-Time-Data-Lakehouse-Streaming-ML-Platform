from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ml_training_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ml"],
) as dag:

    train_model = BashOperator(
        task_id="train_model",
        bash_command="""
        cd /opt/airflow/src &&
        python -m transport_platform.ml.train
        """
    )
