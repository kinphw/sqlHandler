
import pandas as pd
import sqlite3
import os

def import_from_pkl(db_path, file_path, import_scope="all", source_name=None, target_table=None, if_exists="replace"):
    """
    Imports data from a Pickle file into a SQLite database.
    
    Args:
        db_path (str): Path to the SQLite database file.
        file_path (str): Path to the Pickle file.
        import_scope (str): 'single' or 'all'.
        source_name (str): Key name if dict, else ignored for single DF.
        target_table (str): Target table name.
        if_exists (str): 'replace' or 'append'.
    """
    conn = None
    try:
        data = pd.read_pickle(file_path)
        conn = sqlite3.connect(db_path)
        
        if isinstance(data, pd.DataFrame):
            # Single DataFrame
            if not target_table:
                # Use filename as default table name
                base_name = os.path.basename(file_path)
                target_table = os.path.splitext(base_name)[0]
                print(f"ℹ️ 대상 테이블명이 없어 파일명 '{target_table}'을 사용합니다.")
            
            _process_df(conn, data, target_table, if_exists)
            
        elif isinstance(data, dict):
            # Dictionary of DataFrames
            if import_scope == "single":
                if not source_name:
                    raise ValueError("Dictionary 파일에서 특정 데이터를 가져오려면 Key(소스명)가 필요합니다.")
                
                if source_name not in data:
                    raise ValueError(f"Pickle 파일 내에 Key '{source_name}'가 존재하지 않습니다.")
                
                if not target_table:
                    target_table = source_name
                
                df = data[source_name]
                if not isinstance(df, pd.DataFrame):
                     raise ValueError(f"Key '{source_name}'의 데이터가 DataFrame이 아닙니다.")
                
                _process_df(conn, df, target_table, if_exists)
                
            else:
                # Import all
                for key, df in data.items():
                    if isinstance(df, pd.DataFrame):
                        # Sanitize table name
                        table_name = "".join(c if c.isalnum() else "_" for c in str(key))
                        print(f"▶ 처리 중: Key '{key}' -> 테이블 '{table_name}'")
                        _process_df(conn, df, table_name, if_exists)
        else:
            raise ValueError("지원되지 않는 Pickle 데이터 형식입니다. (DataFrame 또는 Dict[str, DataFrame]만 지원)")
            
        conn.commit()
        print(f"✅ SQLite Import 완료: {db_path}")
        return True

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        if conn: conn.rollback()
        raise e
    finally:
        if conn: conn.close()

def _process_df(conn, df, table_name, if_exists):
    """Helper to write DataFrame to SQLite."""
    cursor = conn.cursor()
    
    if if_exists == "replace":
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"   - 기존 테이블 '{table_name}' 삭제됨 (Replace 모드)")
    
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    print(f"   ✓ 데이터 저장 완료: {df.shape[0]} rows")
