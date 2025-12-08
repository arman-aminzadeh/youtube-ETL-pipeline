
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
import logging

logger = logging.getLogger(__name__)
def conn_cursor ():
    hook = PostgresHook(postgres_conn_id= "postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn,cur

def close_conn_cur(conn,cur):
    cur.close()
    conn.close()

def create_schema(schema: str):
    """Create schema if it doesn't exist. Safe to call many times."""
    conn, cur = conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    logger.info("Running SQL: %s", schema_sql)

    try:
        cur.execute(schema_sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("Error in create_schema(%s): %s", schema, e)
        raise
    finally:
        close_conn_cur(conn, cur)


def create_table(schema,table):
    conn, cur = conn_cursor()

    if schema == "staging":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID"       VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title"    TEXT NOT NULL,
                "Upload_Date"    TIMESTAMP NOT NULL,
                "Duration"       VARCHAR(20) NOT NULL,
                "Video_Views"    INT,
                "Likes_Count"    INT,
                "Comments_Count" INT
            );
        """
    else:
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID"       VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title"    TEXT NOT NULL,
                "Upload_Date"    TIMESTAMP NOT NULL,
                "Duration"       TIME NOT NULL,
                "Video_Type"     VARCHAR(10) NOT NULL,
                "Video_Views"    INT,
                "Likes_Count"    INT,
                "Comments_Count" INT
            );
        """

    cur.execute(table_sql)
    conn.commit()
    close_conn_cur(conn, cur)

def get_video_ids(cur,schema,table):
    cur.execute(f"""Select "Video_ID" FROM {schema}.{table}; """)
    ids = cur.fetchall()
    video_ids= [row["Video_ID"] for row in ids]
    return video_ids
