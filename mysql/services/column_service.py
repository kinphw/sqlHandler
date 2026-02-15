from typing import Optional, List, Tuple
import pymysql


def fetch_table_columns(db_config: dict, table_name: str) -> Optional[List[Tuple[str, str, str, str]]]:
    """
    Fetch column info for a MySQL table.
    Returns list of (column_name, data_type, column_key, extra) ordered by position,
    or None if the table does not exist.
    """
    conn = None
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            port=int(db_config['port']),
            database=db_config['database'],
            charset='utf8mb4',
            autocommit=True
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, EXTRA
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ORDINAL_POSITION
                """,
                (db_config['database'], table_name)
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [(r[0], r[1], r[2], r[3]) for r in rows]
    except Exception as e:
        print(f"⚠️ 컬럼 조회 실패 ({table_name}): {e}")
        return None
    finally:
        if conn:
            conn.close()
