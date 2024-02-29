from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ETL.extract_games import download_file
from ETL.transform_games_data import decompress_zst_file, parse_pgn_file,delete_file


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 12),
    'retries': 1,
}

dag = DAG(
    dag_id='lichess_etl_dag',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval as needed
    catchup=False,
)

download_task = PythonOperator(
    task_id='download_lichess_data',
    python_callable=download_file,
    op_args=["https://database.lichess.org/standard/lichess_db_standard_rated_2013-01.pgn.zst", "/opt/airflow/dags/Temp_chess_data"],
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_lichess_data',
    python_callable=decompress_zst_file,
    op_args=['/opt/airflow/dags/Temp_chess_data/lichess_db_standard_rated_2013-01.pgn.zst'],
    provide_context=True,
    dag=dag,
)
parse_task = PythonOperator(
    task_id='parse_pgn_data',
    python_callable=parse_pgn_file,
    op_args=['/opt/airflow/dags/Temp_chess_data/lichess_db_standard_rated_2013-01.pgn'],
    provide_context=True,
    dag=dag,
)

delete_zst_file = PythonOperator(
    task_id = 'delete_zst_file' ,
    python_callable=delete_file,
    op_args=['/opt/airflow/dags/Temp_chess_data/lichess_db_standard_rated_2013-01.pgn.zst'],
    provide_context=True,
    dag=dag,
)

delete_pgn_file = PythonOperator(
    task_id = 'delete_pgn_file' ,
    python_callable=delete_file,
    op_args=['/opt/airflow/dags/Temp_chess_data/lichess_db_standard_rated_2013-01.pgn'],
    provide_context=True,
    dag=dag,
)
# Set up task dependencies
download_task >> transform_task  
transform_task >> delete_zst_file
transform_task>>parse_task
parse_task >> delete_pgn_file