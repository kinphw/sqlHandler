import pandas as pd
from sqlalchemy import create_engine, inspect, text, event
from sqlalchemy.engine.url import make_url
import os

def import_from_xlsx(db_url, file_path, import_scope="all", source_name=None, target_table=None, if_exists="replace", collation="server_default", stop_on_mismatch=True):
    """
    Imports an Excel file to MySQL. Supports both single sheet and full import.
    
    Args:
        db_url (str): SQLAlchemy database URL.
        file_path (str): Path to the Excel file.
        import_scope (str): 'single' for specific sheet, 'all' for full import.
        source_name (str, optional): Sheet name to import (for single mode, None for first sheet).
        target_table (str, optional): Target table name (for single mode).
        if_exists (str): 'replace' to drop existing table, 'append' to add to existing table.
        collation (str): Target collation, or 'server_default'.
        stop_on_mismatch (bool): Stop import when collation mismatch is detected.
    """
    try:
        engine = create_engine(db_url)
        desired_collation = _normalize_collation(collation)
        _configure_engine_collation(engine, desired_collation)
        print(f"✅ [xlsx2mysql] 데이터베이스 연결 성공!")

        db_name = _get_db_name_from_url(db_url)
        schema_collation = _get_schema_collation(engine, db_name) if db_name else None
        selected_text = desired_collation or "server_default"
        if schema_collation:
            print(f"ℹ️ [xlsx2mysql] 선택 콜레이션: {selected_text} (DB 기본: {schema_collation})")
        else:
            print(f"ℹ️ [xlsx2mysql] 선택 콜레이션: {selected_text}")
        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names())
        
        if import_scope == "single":
            # Single sheet import
            if source_name:
                # Specific sheet name provided
                df = pd.read_excel(file_path, sheet_name=source_name)
                print(f"✅ [xlsx2mysql] 시트 '{source_name}' 로딩 완료: {df.shape[0]} rows, {df.shape[1]} columns")
            else:
                # No sheet name → use first sheet
                df = pd.read_excel(file_path, sheet_name=0)
                print(f"✅ [xlsx2mysql] 첫 번째 시트 로딩 완료: {df.shape[0]} rows, {df.shape[1]} columns")
            
            if not target_table:
                raise ValueError("특정 테이블 Import 모드에서는 대상 테이블명이 필요합니다.")
            
            table_existed = target_table in existing_tables
            if table_existed and db_name:
                _report_existing_table_collation(engine, db_name, target_table)
                if desired_collation:
                    mismatch = _report_collation_mismatch(engine, db_name, target_table, desired_collation, schema_collation)
                    if mismatch and stop_on_mismatch:
                        raise ValueError(f"콜레이션 불일치로 중단: 테이블 '{target_table}'")
            elif not table_existed:
                print(f"  ℹ️ 대상 테이블 '{target_table}' 미존재: 신규 생성 예정")

            # Import single table
            _import_single_table(df, target_table, engine, if_exists, desired_collation, table_existed)
            print(f"🎉 [xlsx2mysql] 테이블 '{target_table}' Import 완료!")
            
        else:
            # Full import - all sheets
            print(f"▶ [xlsx2mysql] 엑셀 파일 '{os.path.basename(file_path)}'의 모든 시트 읽는 중...")
            sheets = pd.read_excel(file_path, sheet_name=None)
            
            print(f"✅ [xlsx2mysql] {len(sheets)}개 시트 발견: {', '.join(sheets.keys())}")
            
            for sheet_name, df in sheets.items():
                # Use sheet name as table name (clean it)
                table_name = sheet_name.strip().lower().replace(" ", "_")
                print(f"\n▶ [xlsx2mysql] 시트 '{sheet_name}' → 테이블 '{table_name}' 처리 중... ({df.shape[0]} rows, {df.shape[1]} columns)")
                
                table_existed = table_name in existing_tables
                if table_existed and db_name:
                    _report_existing_table_collation(engine, db_name, table_name)
                    if desired_collation:
                        mismatch = _report_collation_mismatch(engine, db_name, table_name, desired_collation, schema_collation)
                        if mismatch and stop_on_mismatch:
                            raise ValueError(f"콜레이션 불일치로 중단: 테이블 '{table_name}'")

                _import_single_table(df, table_name, engine, if_exists, desired_collation, table_existed)
            
            print(f"\n🎉 [xlsx2mysql] 총 {len(sheets)}개 테이블 Import 완료!")

        return True

    except Exception as e:
        print(f"❌ [xlsx2mysql] 오류 발생: {e}")
        raise e


def _insert_ignore(table, conn, keys, data_iter):
    """Custom insert method for pandas to_sql that uses INSERT IGNORE."""
    from sqlalchemy.dialects.mysql import insert
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).prefix_with("IGNORE").values(data)
    conn.execute(stmt)


def _import_single_table(df, table_name, engine, if_exists, desired_collation, table_existed):
    """Import a single DataFrame to MySQL table."""
    # _x000D_ 처리 (Excel 특수 문자)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.replace('_x000D_', '', regex=False)

    mode_text = "대체" if if_exists == "replace" else "추가"

    if if_exists == "replace":
        if table_existed:
            print(f"  🗑️ 기존 테이블 '{table_name}' 삭제 후 재생성")
        else:
            print(f"  ℹ️ 테이블 '{table_name}' 신규 생성")
    else:
        if table_existed:
            print(f"  ✅ 기존 테이블 '{table_name}'에 데이터 추가 (중복 키 Skip)")
        else:
            print(f"  ℹ️ 테이블 '{table_name}' 신규 생성 후 데이터 삽입")

    print(f"  ▶ Import 중 ({mode_text} 모드)...")
    method = _insert_ignore if (if_exists == "append" and table_existed) else "multi"
    df.to_sql(name=table_name, con=engine, index=False, if_exists=if_exists, method=method)
    _apply_table_collation(engine, table_name, desired_collation, table_existed, if_exists)
    print(f"  ✅ {len(df)} rows Import 완료")


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


def _get_db_name_from_url(db_url):
    try:
        return make_url(db_url).database
    except Exception:
        return None


def _escape_identifier(name):
    return name.replace("`", "``")


def _report_existing_table_collation(engine, db_name, table_name):
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
            print(f"  ℹ️ 기존 테이블 콜레이션: '{table_name}' = {result}")


def _report_collation_mismatch(engine, db_name, table_name, desired_collation, schema_collation):
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
                print(f"  ✅ 테이블 콜레이션 일치: '{table_name}' = {result}")
            else:
                db_default_text = f"DB 기본: {schema_collation}" if schema_collation else "DB 기본: 알 수 없음"
                print(f"  ⚠️ 테이블 콜레이션 불일치: '{table_name}' = {result} (선택: {desired_collation}, {db_default_text})")
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
            print("  ⚠️ 컬럼 콜레이션 불일치 목록:")
            for col_name, collation_name in mismatched:
                print(f"    - {col_name}: {collation_name}")
            has_mismatch = True
    return has_mismatch


def _apply_table_collation(engine, table_name, desired_collation, table_existed, if_exists):
    if not desired_collation:
        return

    if if_exists == "append" and table_existed:
        print("  ℹ️ Append 모드 + 기존 테이블: 콜레이션 변경하지 않고 진행")
        return

    safe_table = _escape_identifier(table_name)
    alter_sql = f"ALTER TABLE `{safe_table}` CONVERT TO CHARACTER SET utf8mb4 COLLATE {desired_collation}"
    with engine.begin() as conn:
        conn.execute(text(alter_sql))
    print(f"  ✅ 콜레이션 적용 완료: {desired_collation}")


def _get_schema_collation(engine, db_name):
    if not db_name:
        return None
    with engine.connect() as conn:
        sql = text(
            """
            SELECT DEFAULT_COLLATION_NAME
            FROM information_schema.schemata
            WHERE schema_name = :db
            """
        )
        return conn.execute(sql, {"db": db_name}).scalar()
