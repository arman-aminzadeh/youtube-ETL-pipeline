
from airflow import DAG
from datetime import datetime,timedelta
from api.video_stats import get_playList_Id,get_videos_ids,get_video_data,data_save_to_json
import pendulum
from datawarehouse.dwh import staging_table, core_table
local_tz = pendulum.timezone('Europe/Stockholm')

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    #"email": ["your_email@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "max_active_runs": 1,
    # "retries": 2,
    # "retry_delay": pendulum.duration(minutes=10),
    "dagrun_timeout":timedelta(hours=1),
    "start_date": pendulum.datetime(2025, 1, 1, tz=local_tz),
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to process json file from raw data',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Define task
    play_list_id = get_playList_Id()
    video_ids = get_videos_ids(play_list_id)
    extract_data = get_video_data(video_ids)
    save_to_json = data_save_to_json(extract_data)

    # Define dependencies

    play_list_id >> video_ids >> extract_data >> save_to_json


with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to process json file and insert data into tables',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Define task
    update_staging = staging_table()
    update_core = core_table()
    # Define dependencies

    update_staging >> update_core
