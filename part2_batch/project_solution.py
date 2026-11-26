"""
Етап 4. Airflow DAG, який послідовно запускає:
landing_to_bronze → bronze_to_silver → silver_to_gold

Кожна таска запускає Spark-скрипт через BashOperator (вбудований в Airflow,
не потребує окремого Spark-провайдера). Скрипти самі створюють SparkSession.

Змінні оточення (необов'язкові, задаються перед `airflow standalone`):
  FP_PYTHON   — Python, у якому встановлено pyspark і requests
                (за замовчуванням — той самий Python, що й Airflow)
  FP_DATA_DIR — папка, де з'являться landing/ bronze/ silver/ gold/
                (за замовчуванням — ~/airflow/fp_data)
"""
import os
import sys
from datetime import datetime

from airflow import DAG

try:  # Airflow 3.x
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator

# Spark-скрипти лежать у тій самій папці, що й цей DAG
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_BIN = os.getenv("FP_PYTHON", sys.executable)
DATA_DIR = os.getenv("FP_DATA_DIR", os.path.expanduser("~/airflow/fp_data"))


def run_script(script_name: str) -> str:
    """Bash-команда: перейти в папку даних і запустити Spark-скрипт."""
    script_path = os.path.join(DAGS_DIR, script_name)
    return f'mkdir -p "{DATA_DIR}" && cd "{DATA_DIR}" && "{PYTHON_BIN}" "{script_path}"'


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="goit_de_fp_batch_datalake",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["goit", "final_project", "datalake"],
) as dag:

    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=run_script("landing_to_bronze.py"),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=run_script("bronze_to_silver.py"),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=run_script("silver_to_gold.py"),
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
