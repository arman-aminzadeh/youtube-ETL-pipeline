
import logging

logger= logging.getLogger(__name__)
table = "yt_api_test"


def insert_rows(conn, cur, schema, table, row):
    try:
        if schema == "staging":
            # row keys (from API / JSON):
            # video_id, title, publishedAt, duration, viewCount, likeCount, commentCount
            video_id_key = "video_id"

            sql = f"""
                INSERT INTO {schema}.{table}
                ("Video_ID","Video_Title","Upload_Date","Duration",
                 "Video_Views","Likes_Count","Comments_Count")
                VALUES (
                    %(video_id)s,
                    %(title)s,
                    %(publishedAt)s,
                    %(duration)s,
                    %(viewCount)s,
                    %(likeCount)s,
                    %(commentCount)s
                );
            """

        else:
            # row keys (from core / transformed dict):
            # Video_ID, Video_Title, Upload_Date, Duration, Video_Type, Video_Views, Likes_Count, Comments_Count
            video_id_key = "Video_ID"

            sql = f"""
                INSERT INTO {schema}.{table}
                ("Video_ID","Video_Title","Upload_Date","Duration",
                 "Video_Type","Video_Views","Likes_Count","Comments_Count")
                VALUES (
                    %(Video_ID)s,
                    %(Video_Title)s,
                    %(Upload_Date)s,
                    %(Duration)s,
                    %(Video_Type)s,
                    %(Video_Views)s,
                    %(Likes_Count)s,
                    %(Comments_Count)s
                );
            """

        cur.execute(sql, row)
        conn.commit()
        logger.info(f"Inserted row with Video_ID: {row[video_id_key]}")

    except Exception as e:
        logger.error(
            f"Error inserting row with Video_ID={row.get(video_id_key)}: {e}"
        )
        raise e

def update_rows(cur,conn,schema,row):
    try:
        if schema == "staging":

            video_id = "video_id"
            upload_date= "publishedAt"
            video_title = "title"
            video_views= "viewCount"
            likes_count="likeCount"
            comments_count = "commentCount"
        #core
        else:
            video_id = "Video_ID"
            upload_date= "Upload_Date"
            video_title = "Video_Title"
            video_views= "Video_Views"
            likes_count="Likes_Count"
            comments_count = "Comment_Count"

        cur.execute(
            f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s,
                "Likes_Count" = %({likes_count})s,
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """,row
        )
        conn.commit()
        logger.info(f"Updated row with Video_ID:{row[video_id]}")
    except Exception as e:
        logger.error(f"error updating row with Video_ID:{row[video_id]} - {e}")
        raise e


def delete_rows(cur, conn, schema, table, ids_to_delete):
    try:
        sql = f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" = ANY(%s)
        """
        cur.execute(sql, (ids_to_delete,))
        conn.commit()
        logger.info(f"Deleted {cur.rowcount} rows")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting rows: {e}")
        raise
