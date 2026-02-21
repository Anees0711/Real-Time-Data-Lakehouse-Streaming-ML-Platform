from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Ensure transport_platform is importable inside tasks
TASK_ENV = dict(os.environ)
TASK_ENV["PYTHONPATH"] = TASK_ENV.get("PYTHONPATH", "/opt/airflow/src")

with DAG(
    dag_id="fr_transport_realtime_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",  # hourly
    catchup=False,
    tags=["realtime", "lakehouse"],
) as dag:
    # Optional: quick import check to fail fast with a clear error
    import_check = BashOperator(
        task_id="import_check",
        bash_command="bash -lc 'python -c \"import transport_platform; print(transport_platform.__file__)\"'",
        env=TASK_ENV,
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command="bash -lc 'python -m transport_platform.batch.build_silver'",
        env=TASK_ENV,
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command="bash -lc 'python -m transport_platform.quality.validate_silver'",
        env=TASK_ENV,
    )

    build_gold = BashOperator(
        task_id="build_gold_features",
        bash_command="bash -lc 'python -m transport_platform.batch.build_gold_features'",
        env=TASK_ENV,
    )

    check_gold = BashOperator(
        task_id="check_gold",
        bash_command="bash -lc 'python -m transport_platform.batch.check_gold'",
        env=TASK_ENV,
    )

    # Keep loaders/ML as placeholders if you haven’t created them yet.
    # You can add them later once gold is confirmed working.

    import_check >> build_silver >> validate_silver >> build_gold >> check_gold
