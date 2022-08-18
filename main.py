from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


conn = snowflake.connector.connect(
    user=Variable.get('user'),
    password=Variable.get('password'),
    account=Variable.get('account'),
    warehouse=Variable.get('warehouse'),
    database=Variable.get('database'),
    schema=Variable.get('schema')
)

cursor = conn.cursor()


def upload_csv(**kwargs) -> bool:
    data = pd.read_csv(Variable.get('csv_file'), nrows=1)

    data.columns = map(lambda x: str(x).upper(), data.columns)
    columns_names = ''.join([f'{i} string, ' for i in data.columns])[:-2]

    ti = kwargs["ti"]
    ti.xcom_push("columns_names", columns_names)
    ti.xcom_push("data", data.to_json())
    return True


def create_table_and_stream(ti: TaskInstance) -> bool:
    columns_names = ti.xcom_pull(task_ids="upload_csv", key="columns_names")

    create_raw_table = \
        f"""CREATE OR REPLACE TABLE 
        {Variable.get('database')}.{Variable.get('schema')}.RAW_TABLE ({columns_names})"""

    cursor.execute(create_raw_table)

    cursor.execute("CREATE OR REPLACE TABLE STAGE_TABLE LIKE RAW_TABLE")

    cursor.execute("CREATE OR REPLACE TABLE MASTER_TABLE LIKE STAGE_TABLE")

    cursor.execute("CREATE OR REPLACE STREAM RAW_STREAM ON TABLE RAW_TABLE")
    cursor.execute("CREATE OR REPLACE STREAM STAGE_STREAM ON TABLE STAGE_TABLE")
    return True


def write_to_snowflake(ti: TaskInstance) -> bool:
    data = ti.xcom_pull(task_ids="upload_csv", key="data")

    data = pd.read_json(data)

    write_pandas(conn, data, table_name='RAW_TABLE')
    return True


def stream_to_stage_table(ti: TaskInstance) -> bool:
    columns_names = ti.xcom_pull(task_ids="upload_csv", key="columns_names")

    columns_names = columns_names.lstrip("string")
    cursor.execute(f"INSERT INTO STAGE_TABLE (SELECT {columns_names} FROM RAW_STREAM)")
    return True


def stream_to_master_table(ti: TaskInstance) -> bool:
    columns_names = ti.xcom_pull(task_ids="upload_csv", key="columns_names")
    columns_names = columns_names.lstrip("string")
    cursor.execute(f"INSERT INTO MASTER_TABLE (SELECT {columns_names} FROM STAGE_STREAM)")
    cursor.close()
    return True


with DAG(
    'SnowflakeTask',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='Task 6. Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Snowflake'],
) as dag:

    t1 = PythonOperator(
        task_id='upload_csv',
        python_callable=upload_csv,
    )

    t2 = PythonOperator(
        task_id='create_table_and_stream',
        python_callable=create_table_and_stream,
    )

    t3 = PythonOperator(
        task_id='write_to_snowflake',
        python_callable=write_to_snowflake,
    )

    t4 = PythonOperator(
        task_id='stream_to_stage_table',
        python_callable=stream_to_stage_table,
    )

    t5 = PythonOperator(
        task_id='stream_to_master_table',
        python_callable=stream_to_master_table,
    )

    [t1, t2] >> t3 >> t4 >> t5
