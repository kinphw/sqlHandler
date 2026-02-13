
import pandas as pd
import sqlite3
import os

def import_from_xlsx(db_path, file_path, import_scope="all", source_name=None, target_table=None, if_exists="replace"):
    """
    Imports data from an Excel file into a SQLite database.
    
    Args:
        db_path (str): Path to the SQLite database file.
        file_path (str): Path to the Excel file.
        import_scope (str): 'single' (one sheet) or 'all' (all sheets).
        source_name (str): Sheet name to import (if scope is 'single').
        target_table (str): Target table name (if scope is 'single').
        if_exists (str): 'replace' to drop/create, 'append' to add data.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        excel = pd.ExcelFile(file_path)
        
        if import_scope == "single":
            # Determine sheet name
            sheet_name = source_name if source_name else excel.sheet_names[0]
            if sheet_name not in excel.sheet_names:
                raise ValueError(f"시트 '{sheet_name}'를 찾을 수 없습니다.")
            
            # Determine table name
            if not target_table:
                raise ValueError("대상 테이블명이 지정되지 않았습니다.")
            
            _process_sheet(conn, excel, sheet_name, target_table, if_exists)
            
        else:
            # Import all sheets
            for sheet_name in excel.sheet_names:
                # Sanitize table name: keep alphanumeric, replace others with _
                table_name = "".join(c if c.isalnum() else "_" for c in sheet_name)
                print(f"▶ 처리 중: 시트 '{sheet_name}' -> 테이블 '{table_name}'")
                
                _process_sheet(conn, excel, sheet_name, table_name, if_exists)
                
        conn.commit()
        print(f"✅ SQLite Import 완료: {db_path}")
        return True
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        if conn: conn.rollback()
        raise e
    finally:
        if conn: conn.close()
        # excel reference closed automatically

def _process_sheet(conn, excel, sheet_name, table_name, if_exists):
    """Helper to process a single sheet."""
    cursor = conn.cursor()
    
    # Read Excel
    df = pd.read_excel(excel, sheet_name=sheet_name)
    
    if if_exists == "replace":
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"   - 기존 테이블 '{table_name}' 삭제됨 (Replace 모드)")
    
    # Write to SQLite
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    print(f"   ✓ 데이터 저장 완료: {df.shape[0]} rows")
