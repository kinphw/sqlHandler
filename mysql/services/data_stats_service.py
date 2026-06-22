from typing import Optional, List, Tuple
import pymysql


def _connect(db_config: dict):
    return pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        port=int(db_config['port']),
        database=db_config['database'],
        charset='utf8mb4',
        autocommit=True,
    )


def _esc(identifier: str) -> str:
    """Escape a MySQL identifier for use inside backticks."""
    return identifier.replace("`", "``")


def fetch_table_row_count(db_config: dict, table_name: str) -> Optional[int]:
    """Exact COUNT(*) for a table. Returns None if the table is missing or on error
    (None lets the caller distinguish '존재하지 않음' from '0 rows')."""
    conn = None
    try:
        conn = _connect(db_config)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                """,
                (db_config['database'], table_name),
            )
            if not cur.fetchone()[0]:
                return None
            cur.execute(f"SELECT COUNT(*) FROM `{_esc(table_name)}`")
            return int(cur.fetchone()[0])
    except Exception as e:
        print(f"⚠️ 행 수 조회 실패 ({table_name}): {e}")
        return None
    finally:
        if conn:
            conn.close()


def fetch_all_table_row_counts(db_config: dict) -> List[Tuple[str, int]]:
    """Return [(table_name, exact_row_count), ...] for every base table in the DB.
    A count of -1 means COUNT(*) failed for that single table."""
    conn = None
    try:
        conn = _connect(db_config)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (db_config['database'],),
            )
            tables = [r[0] for r in cur.fetchall()]
            result: List[Tuple[str, int]] = []
            for t in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM `{_esc(t)}`")
                    result.append((t, int(cur.fetchone()[0])))
                except Exception:
                    result.append((t, -1))
            return result
    except Exception as e:
        print(f"⚠️ 전체 테이블 행 수 조회 실패: {e}")
        return []
    finally:
        if conn:
            conn.close()


def fetch_query_row_count(db_config: dict, query: str) -> Optional[int]:
    """COUNT(*) over a read-only query (wrapped as a derived table).
    Returns None on error (e.g. query not wrappable). Caller should already have
    validated the query as read-only."""
    conn = None
    try:
        normalized = (query or "").strip().rstrip(";").strip()
        if not normalized:
            return None
        conn = _connect(db_config)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM ({normalized}) AS _cnt_sub")
            return int(cur.fetchone()[0])
    except Exception as e:
        print(f"⚠️ 쿼리 행 수 조회 실패: {e}")
        return None
    finally:
        if conn:
            conn.close()


def fetch_key_columns(db_config: dict, table_name: str) -> List[str]:
    """Return PRIMARY KEY columns (ordered). Falls back to the columns of the first
    UNIQUE index when no PK exists. Empty list if neither exists / on error."""
    conn = None
    try:
        conn = _connect(db_config)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COLUMN_NAME FROM information_schema.statistics
                WHERE table_schema = %s AND table_name = %s AND index_name = 'PRIMARY'
                ORDER BY SEQ_IN_INDEX
                """,
                (db_config['database'], table_name),
            )
            pk = [r[0] for r in cur.fetchall()]
            if pk:
                return pk

            cur.execute(
                """
                SELECT index_name, COLUMN_NAME, SEQ_IN_INDEX
                FROM information_schema.statistics
                WHERE table_schema = %s AND table_name = %s AND non_unique = 0
                ORDER BY index_name, SEQ_IN_INDEX
                """,
                (db_config['database'], table_name),
            )
            rows = cur.fetchall()
            if not rows:
                return []
            first_index = rows[0][0]
            return [r[1] for r in rows if r[0] == first_index]
    except Exception as e:
        print(f"⚠️ 키 컬럼 조회 실패 ({table_name}): {e}")
        return []
    finally:
        if conn:
            conn.close()


def fetch_table_sample(db_config: dict, table_name: str, limit: int = 500):
    """Return (columns, rows) for the first `limit` rows of a table, or None on error."""
    conn = None
    try:
        conn = _connect(db_config)
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM `{_esc(table_name)}` LIMIT {int(limit)}")
            columns = [d[0] for d in cur.description]
            rows = list(cur.fetchall())
        return columns, rows
    except Exception as e:
        print(f"⚠️ 테이블 샘플 조회 실패 ({table_name}): {e}")
        return None
    finally:
        if conn:
            conn.close()


def fetch_query_sample(db_config: dict, query: str, limit: int = 500):
    """Return (columns, rows) for the first `limit` rows of a read-only query, or None.
    Tries to wrap the query as a derived table with LIMIT; falls back to a raw execute."""
    conn = None
    try:
        normalized = (query or "").strip().rstrip(";").strip()
        if not normalized:
            return None
        conn = _connect(db_config)
        with conn.cursor() as cur:
            try:
                cur.execute(f"SELECT * FROM ({normalized}) AS _pv_sub LIMIT {int(limit)}")
            except Exception:
                cur.execute(normalized)
            columns = [d[0] for d in cur.description]
            rows = list(cur.fetchmany(int(limit)))
        return columns, rows
    except Exception as e:
        print(f"⚠️ 쿼리 샘플 조회 실패: {e}")
        return None
    finally:
        if conn:
            conn.close()


def fetch_existing_key_set(
    db_config: dict,
    table_name: str,
    key_columns: List[str],
    key_tuples: List[tuple],
) -> set:
    """Return the subset of the given key tuples that already exist in the table.
    Used for per-row '기적재(이미 적재됨)' marking. Empty set on error/empty input."""
    if not key_columns or not key_tuples:
        return set()
    conn = None
    try:
        distinct = list({t for t in key_tuples})
        conn = _connect(db_config)
        safe_cols = ", ".join(f"`{_esc(c)}`" for c in key_columns)
        group = "(" + ",".join(["%s"] * len(key_columns)) + ")"
        safe_table = _esc(table_name)
        chunk_size = 500
        found = set()
        with conn.cursor() as cur:
            for i in range(0, len(distinct), chunk_size):
                chunk = distinct[i:i + chunk_size]
                placeholders = ",".join([group] * len(chunk))
                sql = f"SELECT {safe_cols} FROM `{safe_table}` WHERE ({safe_cols}) IN ({placeholders})"
                flat = [v for tup in chunk for v in tup]
                cur.execute(sql, flat)
                for r in cur.fetchall():
                    found.add(tuple(r))
        return found
    except Exception as e:
        print(f"⚠️ 기적재 키 조회 실패 ({table_name}): {e}")
        return set()
    finally:
        if conn:
            conn.close()


def count_existing_keys(
    db_config: dict,
    table_name: str,
    key_columns: List[str],
    key_tuples: List[tuple],
) -> Optional[int]:
    """Count how many of the given (distinct) key tuples already exist in the table.
    Uses chunked row-constructor IN queries. Returns None on error."""
    if not key_columns or not key_tuples:
        return 0
    conn = None
    try:
        conn = _connect(db_config)
        safe_cols = ", ".join(f"`{_esc(c)}`" for c in key_columns)
        group = "(" + ",".join(["%s"] * len(key_columns)) + ")"
        safe_table = _esc(table_name)
        chunk_size = 500
        total = 0
        with conn.cursor() as cur:
            for i in range(0, len(key_tuples), chunk_size):
                chunk = key_tuples[i:i + chunk_size]
                placeholders = ",".join([group] * len(chunk))
                sql = f"SELECT COUNT(*) FROM `{safe_table}` WHERE ({safe_cols}) IN ({placeholders})"
                flat = [v for tup in chunk for v in tup]
                cur.execute(sql, flat)
                total += int(cur.fetchone()[0])
        return total
    except Exception as e:
        print(f"⚠️ 중복 키 조회 실패 ({table_name}): {e}")
        return None
    finally:
        if conn:
            conn.close()
