# Airflow DAG Examples

This repository contains sample DAGs for Apache Airflow demonstrating simple ETL workflows.

## Included DAGs

- **`get_users.py`** – Downloads random user data from `randomuser.me`, processes it and saves a simplified JSON file.
- **`main_dag_bash_demo_v1.py`** – ETL pipeline for the Iris dataset using `PythonOperator` tasks for extraction and transformation and a `BashOperator` for the load step.
- **`other_dag.py`** – Retrieves several indicators from the Brazilian Central Bank's SGS system and merges them. Runs every Monday at 8 AM.

## Setup

1. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```
2. Initialize Airflow and start the webserver and scheduler. Refer to the [Airflow documentation](https://airflow.apache.org/docs/) for details.

These DAGs expect temporary CSV outputs under `/tmp`. Adjust paths as needed or mount persistent storage in production.
