import pandas as pd
from sqlalchemy import inspect, text, event
import os
from mysql.services.engine_factory import create_mysql_engine, dispose_mysql_engine

def import_from_pkl(db_config, file_path, import_scope="all", source_name=None, target_table=None, if_exists="replace", collation="server_default", stop_on_mismatch=True, excluded_columns=None, logger=None):
    """
    Imports a Pickle file to MySQL. Supports both single table and full import.

    Args:
        db_config (dict): Dictionary with keys 'host', 'port', 'user', 'password', 'database'.
        file_path (str): Path to the Pickle file.
        import_scope (str): 'single' for specific table, 'all' for full import.
        source_name (str, optional): Dictionary key to extract (for single mode with dict pickle).
        target_table (str, optional): Target table name (for single mode).
        if_exists (str): 'replace' to drop existing table, 'append' to add to existing table.
        collation (str): Target collation, or 'server_default'.
        stop_on_mismatch (bool): Stop import when collation mismatch is detected.
        excluded_columns (dict, optional): {table_name: [col_names_to_exclude]}.
        logger (callable, optional): Logging function. Defaults to print.
    """
    log = logger or print
    engine = None
    try:
        db_url = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{int(db_config['port'])}/{db_config['database']}?charset=utf8mb4"
        )
        engine = create_mysql_engine(db_url)
        desired_collation = _normalize_collation(collation)
        _configure_engine_collation(engine, desired_collation)
        schema_collation = _get_schema_collation(engine, db_config['database'])
        selected_text = desired_collation or "server_default"
        if schema_collation:
            log(f"ℹ️ [pkl2mysql] 선택 콜레이션: {selected_text} (DB 기본: {schema_collation})")
        else:
            log(f"ℹ️ [pkl2mysql] 선택 콜레이션: {selected_text}")
        log(f"✅ [pkl2mysql] 데이터베이스 연결 성공!")

        # Load pickle file
        data = pd.read_pickle(file_path)

        # Determine tables to import based on scope
        if import_scope == "single":
            # Single table import
            if isinstance(data, dict):
                # Dictionary: Extract specific key
                if source_name:
                    if source_name not in data:
                        raise ValueError(f"키 '{source_name}'을 찾을 수 없습니다. 사용 가능한 키: {', '.join(data.keys())}")
                    df = data[source_name]
                    log(f"✅ [pkl2mysql] Dictionary에서 키 '{source_name}' 추출 완료: {df.shape[0]} rows, {df.shape[1]} columns")
                else:
                    raise ValueError("Dictionary Pickle에서 특정 테이블을 Import하려면 소스 지정(키)이 필요합니다.")
            else:
                # DataFrame: Use as-is
                df = data
                log(f"✅ [pkl2mysql] DataFrame 로딩 완료: {df.shape[0]} rows, {df.shape[1]} columns")

            if not target_table:
                raise ValueError("특정 테이블 Import 모드에서는 대상 테이블명이 필요합니다.")

            tables_to_import = {target_table: df}

        else:
            # Full import
            if isinstance(data, dict):
                # Dictionary: Use all key-value pairs
                log(f"✅ [pkl2mysql] Dictionary 형식 Pickle 로딩 완료: {len(data)}개 테이블")
                tables_to_import = data
            else:
                # DataFrame: Use filename as table name
                df = data
                log(f"✅ [pkl2mysql] DataFrame 로딩 완료: {df.shape[0]} rows, {df.shape[1]} columns")
                table_name = os.path.basename(file_path).split('.')[0]
                log(f"ℹ️ [pkl2mysql] 파일명을 테이블명으로 사용: '{table_name}'")
                tables_to_import = {table_name: df}

        # Process each table
        imported_count = 0
        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names())

        for tbl_name, df in tables_to_import.items():
            log(f"\n▶ [pkl2mysql] 테이블 '{tbl_name}' 처리 중... ({df.shape[0]} rows, {df.shape[1]} columns)")

            # Clean column names
            df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

            # Drop excluded columns
            cols_to_drop = []
            if excluded_columns and tbl_name in excluded_columns:
                cols_to_drop = [c for c in excluded_columns[tbl_name] if c in df.columns]
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)
                    log(f"  ⏭️ 제외된 컬럼: {', '.join(cols_to_drop)}")

            table_existed = tbl_name in existing_tables
            if table_existed:
                _report_existing_table_collation(engine, db_config['database'], tbl_name, log)
                if desired_collation:
                    mismatch = _report_collation_mismatch(engine, db_config['database'], tbl_name, desired_collation, schema_collation, log)
                    if mismatch and stop_on_mismatch:
                        raise ValueError(f"콜레이션 불일치로 중단: 테이블 '{tbl_name}'")
            elif import_scope == "single":
                log(f"  ℹ️ 대상 테이블 '{tbl_name}' 미존재: 신규 생성 예정")

            # Replace + existing table + excluded columns → transactional delete + append
            effective_if_exists = if_exists
            preserve_existing_schema = bool(if_exists == "replace" and table_existed and cols_to_drop)
            if preserve_existing_schema:
                effective_if_exists = "append"

            # Import based on if_exists mode using pandas.to_sql
            _import_single_table(
                df,
                tbl_name,
                engine,
                effective_if_exists,
                desired_collation,
                table_existed,
                log,
                preserve_existing_schema=preserve_existing_schema,
            )

            imported_count += 1

        scope_text = f"'{target_table}'" if import_scope == "single" else f"{imported_count}개 테이블"
        log(f"\n🎉 [pkl2mysql] {scope_text} Import 완료!")
        return True

    except Exception as e:
        log(f"❌ [pkl2mysql] 오류 발생: {e}")
        raise e
    finally:
        dispose_mysql_engine(engine, logger=log, label="pkl2mysql")


def _insert_ignore(table, conn, keys, data_iter):
    """Custom insert method for pandas to_sql that uses INSERT IGNORE."""
    from sqlalchemy.dialects.mysql import insert
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).prefix_with("IGNORE").values(data)
    conn.execute(stmt)


def _import_single_table(
    df,
    table_name,
    engine,
    if_exists,
    desired_collation,
    table_existed,
    log=print,
    preserve_existing_schema=False,
):
    """Import a single DataFrame to MySQL table using pandas.to_sql."""
    # Clean column names
    df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

    # _x000D_ 처리 (Excel 특수 문자와 동일하게 정리)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.replace('_x000D_', '', regex=False)

    requested_mode_text = "대체" if (if_exists == "replace" or preserve_existing_schema) else "추가"

    if preserve_existing_schema:
        log(f"  ♻️ 기존 테이블 '{table_name}' 구조를 유지한 채 데이터를 교체")
    elif if_exists == "replace":
        if table_existed:
            log(f"  🗑️ 기존 테이블 '{table_name}' 삭제 후 재생성")
        else:
            log(f"  ℹ️ 테이블 '{table_name}' 신규 생성")
    else:
        if table_existed:
            log(f"  ✅ 기존 테이블 '{table_name}'에 데이터 추가 (중복 키 Skip)")
        else:
            log(f"  ℹ️ 테이블 '{table_name}' 신규 생성 후 데이터 삽입")

    log(f"  ▶ Import 중 ({requested_mode_text} 모드)...")

    if preserve_existing_schema:
        _replace_existing_rows_in_transaction(df, table_name, engine, desired_collation, log)
        return

    method = _insert_ignore if (if_exists == "append" and table_existed) else "multi"
    df.to_sql(name=table_name, con=engine, index=False, if_exists=if_exists, method=method)
    _apply_table_collation(engine, table_name, desired_collation, table_existed, if_exists, log)
    log(f"  ✅ {len(df)} rows Import 완료")


def _normalize_collation(collation):
    if not collation or collation == "server_default":
        return None
    return collation


def _configure_engine_collation(engine, desired_collation):
    if not desired_collation:
        return

    if getattr(engine, "_sqlhandler_collation", None) == desired_collation:
        return

    engine._sqlhandler_collation = desired_collation

    @event.listens_for(engine, "connect")
    def _set_collation(dbapi_connection, _):
        cursor = dbapi_connection.cursor()
        cursor.execute(f"SET NAMES utf8mb4 COLLATE {desired_collation}")
        cursor.execute(f"SET SESSION collation_connection='{desired_collation}'")
        cursor.close()


def _delete_all_rows(conn, table_name):
    safe_table = _escape_identifier(table_name)
    conn.execute(text(f"DELETE FROM `{safe_table}`"))


def _replace_existing_rows_in_transaction(df, table_name, engine, desired_collation, log=print):
    """Replace table data while keeping schema and allowing rollback on failure."""
    method = _insert_ignore
    with engine.begin() as conn:
        _delete_all_rows(conn, table_name)
        log(f"  🗑️ 기존 테이블 '{table_name}' 데이터 삭제 (트랜잭션 적용)")
        df.to_sql(name=table_name, con=conn, index=False, if_exists="append", method=method)
        _apply_table_collation(conn, table_name, desired_collation, True, "append", log)
    log(f"  ✅ {len(df)} rows Import 완료")


def _escape_identifier(name):
    return name.replace("`", "``")


def _report_existing_table_collation(engine, db_name, table_name, log=print):
    with engine.connect() as conn:
        table_sql = text(
            """
            SELECT TABLE_COLLATION
            FROM information_schema.tables
            WHERE table_schema = :db AND table_name = :tbl
            """
        )
        result = conn.execute(table_sql, {"db": db_name, "tbl": table_name}).scalar()
        if result:
            log(f"  ℹ️ 기존 테이블 콜레이션: '{table_name}' = {result}")


def _report_collation_mismatch(engine, db_name, table_name, desired_collation, schema_collation, log=print):
    has_mismatch = False
    with engine.connect() as conn:
        table_sql = text(
            """
            SELECT TABLE_COLLATION
            FROM information_schema.tables
            WHERE table_schema = :db AND table_name = :tbl
            """
        )
        result = conn.execute(table_sql, {"db": db_name, "tbl": table_name}).scalar()
        if result:
            if result == desired_collation:
                log(f"  ✅ 테이블 콜레이션 일치: '{table_name}' = {result}")
            else:
                db_default_text = f"DB 기본: {schema_collation}" if schema_collation else "DB 기본: 알 수 없음"
                log(f"  ⚠️ 테이블 콜레이션 불일치: '{table_name}' = {result} (선택: {desired_collation}, {db_default_text})")
                has_mismatch = True

        col_sql = text(
            """
            SELECT column_name, collation_name
            FROM information_schema.columns
            WHERE table_schema = :db AND table_name = :tbl AND collation_name IS NOT NULL
            """
        )
        rows = conn.execute(col_sql, {"db": db_name, "tbl": table_name}).fetchall()
        mismatched = [(r[0], r[1]) for r in rows if r[1] and r[1] != desired_collation]
        if mismatched:
            log("  ⚠️ 컬럼 콜레이션 불일치 목록:")
            for col_name, collation_name in mismatched:
                log(f"    - {col_name}: {collation_name}")
            has_mismatch = True
    return has_mismatch


def _apply_table_collation(connectable, table_name, desired_collation, table_existed, if_exists, log=print):
    if not desired_collation:
        return

    if if_exists == "append" and table_existed:
        log("  ℹ️ Append 모드 + 기존 테이블: 콜레이션 변경하지 않고 진행")
        return

    safe_table = _escape_identifier(table_name)
    alter_sql = f"ALTER TABLE `{safe_table}` CONVERT TO CHARACTER SET utf8mb4 COLLATE {desired_collation}"
    if hasattr(connectable, "execute"):
        connectable.execute(text(alter_sql))
    else:
        with connectable.begin() as conn:
            conn.execute(text(alter_sql))
    log(f"  ✅ 콜레이션 적용 완료: {desired_collation}")


def _get_schema_collation(engine, db_name):
    with engine.connect() as conn:
        sql = text(
            """
            SELECT DEFAULT_COLLATION_NAME
            FROM information_schema.schemata
            WHERE schema_name = :db
            """
        )
        return conn.execute(sql, {"db": db_name}).scalar()
