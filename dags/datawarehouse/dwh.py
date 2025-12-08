
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_date
from datawarehouse.data_utils import conn_cursor,close_conn_cur, create_schema,create_table, get_video_ids

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api_test"

@task
def staging_table():
    schema="staging"
    conn,cur= None, None

    try:
        conn,cur = conn_cursor()
        YT_data = load_data()
        create_schema(schema)
        create_table(schema,table)
        table_ids = get_video_ids(cur,schema,table)

        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(conn,cur,schema,table,row)
            else:
                if row["video_id"] in table_ids:
                    update_rows(cur,conn,schema,row)
                else:
                    insert_rows(conn,cur,schema,table,row)

        logger.info(f"{schema} table update completed..")
    except Exception as e:
        logger.error(f"error occured during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cur(conn,cur)

@task
def core_table():
    schema="core"
    conn,cur= None, None
    
    try:
        conn,cur = conn_cursor()
        create_schema(schema)
        create_table(schema,table)
        table_ids = get_video_ids(cur,schema,table)

        current_video_ids=[]

        cur.execute(f"SELECT * FROM staging.{table}")
        rows = cur.fetchall()
        for row in rows:
            current_video_ids.append(row["Video_ID"])
            if len(table_ids) == 0:
                transform_row = transform_date(row)
                insert_rows(conn,cur,schema,table,transform_row)
            else:
                transform_row = transform_date(row)
                if transform_row["Video_ID"] in table_ids:
                    update_rows(cur,conn,schema,transform_row)
                else:
                    insert_rows(conn,cur,schema,table,transform_row)
            
        logger.info(f"{schema} table update completed..")
    except Exception as e:
        logger.error(f"error occured during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cur(conn,cur)
