from airflow.models import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pendulum

with DAG(
    'sql_server_cnx_dag',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='DAG que conecta con una BD de SQL Server',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:

    def mssql_func(**kwargs):
        conn = MsSqlHook.get_connection(conn_id="mssql_local")
        hook = conn.get_hook()
        df = hook.get_pandas_df(sql="SELECT * FROM [TestDB].[dbo].[ClientData]")
        # do whatever you need on the df
        print(df)

    run_this = PythonOperator(
        task_id='mssql_task',
        python_callable=mssql_func,
        dag=dag
    )
