from typing import Optional, Tuple
import pymysql


def fetch_server_collations(db_config: dict) -> Tuple[Optional[list], Optional[str]]:
    """Return (collations, db_default_collation) or (None, None) on failure."""
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
                "SELECT DEFAULT_COLLATION_NAME FROM information_schema.schemata WHERE schema_name=%s",
                (db_config['database'],)
            )
            row = cur.fetchone()
            db_default = row[0] if row else None

            cur.execute(
                """
                SELECT COLLATION_NAME
                FROM information_schema.COLLATIONS
                WHERE CHARACTER_SET_NAME = 'utf8mb4'
                ORDER BY COLLATION_NAME
                """
            )
            rows = cur.fetchall()
            collations = [r[0] for r in rows if r and r[0]]

        return collations, db_default
    except Exception as e:
        print(f"⚠️ Collation 목록 조회 실패: {e}")
        return None, None
    finally:
        if conn:
            conn.close()


def fetch_table_collation_info(db_config: dict, table_name: str, compare_collation: Optional[str]) -> Tuple[Optional[str], int]:
    """Return (table_collation, mismatched_column_count)."""
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
                SELECT TABLE_COLLATION
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                """,
                (db_config['database'], table_name)
            )
            row = cur.fetchone()
            table_collation = row[0] if row else None

            mismatch_count = 0
            if compare_collation:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                      AND collation_name IS NOT NULL
                      AND collation_name <> %s
                    """,
                    (db_config['database'], table_name, compare_collation)
                )
                mismatch_row = cur.fetchone()
                mismatch_count = mismatch_row[0] if mismatch_row and mismatch_row[0] else 0

        return table_collation, mismatch_count
    except Exception as e:
        print(f"⚠️ 테이블 콜레이션 조회 실패: {e}")
        return None, 0
    finally:
        if conn:
            conn.close()
