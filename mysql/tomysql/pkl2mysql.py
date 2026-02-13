import pandas as pd
import pymysql
import os

def import_from_pkl(db_config, file_path, import_scope="all", source_name=None, target_table=None, if_exists="replace"):
    """
    Imports a Pickle file to MySQL. Supports both single table and full import.
    
    Args:
        db_config (dict): Dictionary with keys 'host', 'port', 'user', 'password', 'database'.
        file_path (str): Path to the Pickle file.
        import_scope (str): 'single' for specific table, 'all' for full import.
        source_name (str, optional): Dictionary key to extract (for single mode with dict pickle).
        target_table (str, optional): Target table name (for single mode).
        if_exists (str): 'replace' to drop existing table, 'append' to add to existing table.
    """
    conn = None
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
        
        print(f"✅ [pkl2mysql] 데이터베이스 연결 성공!")
        
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
                    print(f"✅ [pkl2mysql] Dictionary에서 키 '{source_name}' 추출 완료: {df.shape[0]} rows, {df.shape[1]} columns")
                else:
                    raise ValueError("Dictionary Pickle에서 특정 테이블을 Import하려면 소스 지정(키)이 필요합니다.")
            else:
                # DataFrame: Use as-is
                df = data
                print(f"✅ [pkl2mysql] DataFrame 로딩 완료: {df.shape[0]} rows, {df.shape[1]} columns")
            
            if not target_table:
                raise ValueError("특정 테이블 Import 모드에서는 대상 테이블명이 필요합니다.")
            
            tables_to_import = {target_table: df}
            
        else:
            # Full import
            if isinstance(data, dict):
                # Dictionary: Use all key-value pairs
                print(f"✅ [pkl2mysql] Dictionary 형식 Pickle 로딩 완료: {len(data)}개 테이블")
                tables_to_import = data
            else:
                # DataFrame: Use filename as table name
                df = data
                print(f"✅ [pkl2mysql] DataFrame 로딩 완료: {df.shape[0]} rows, {df.shape[1]} columns")
                table_name = os.path.basename(file_path).split('.')[0]
                print(f"ℹ️ [pkl2mysql] 파일명을 테이블명으로 사용: '{table_name}'")
                tables_to_import = {table_name: df}
        
        # Process each table
        imported_count = 0
        for tbl_name, df in tables_to_import.items():
            print(f"\n▶ [pkl2mysql] 테이블 '{tbl_name}' 처리 중... ({df.shape[0]} rows, {df.shape[1]} columns)")
            
            # Clean column names
            df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]
            
            # Import based on if_exists mode
            if if_exists == "replace":
                _import_replace(tbl_name, df, conn)
            else:  # append
                _import_append(tbl_name, df, conn)
            
            imported_count += 1
        
        scope_text = f"'{target_table}'" if import_scope == "single" else f"{imported_count}개 테이블"
        print(f"\n🎉 [pkl2mysql] {scope_text} Import 완료!")
        return True

    except Exception as e:
        print(f"❌ [pkl2mysql] 오류 발생: {e}")
        raise e
    finally:
        if conn:
            conn.close()


def _import_replace(table_name, df, conn):
    """Replace mode: Drop existing table and create new one."""
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    table_existed = cursor.fetchone() is not None
    
    if table_existed:
        # Drop existing table
        cursor.execute(f"DROP TABLE `{table_name}`")
        print(f"  🗑️ 기존 테이블 '{table_name}' 삭제 완료")
    else:
        print(f"  ℹ️ 테이블 '{table_name}'이 존재하지 않음 (신규 생성)")
    
    # Create table
    _create_table(table_name, df, cursor)
    conn.commit()
    print(f"  🛠️ 테이블 '{table_name}' 생성 완료")
    
    # Insert data
    _insert_data(table_name, df, conn)


def _import_append(table_name, df, conn):
    """Append mode: Insert into existing table (create if not exists)."""
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cursor.fetchone()
    
    if not result:
        print(f"  ℹ️ 테이블 '{table_name}'이 존재하지 않아 생성합니다.")
        _create_table(table_name, df, cursor)
        conn.commit()
        print(f"  🛠️ 테이블 '{table_name}' 생성 완료")
    else:
        print(f"  ✅ 테이블 '{table_name}'이 이미 존재합니다. 데이터를 추가합니다.")
    
    # Insert data (with IGNORE to skip duplicates)
    _insert_ignore(table_name, df, conn)


def _create_table(table_name, df, cursor):
    """Create table based on DataFrame schema."""
    def map_dtype(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "INT"
        elif pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"
        else:
            return "TEXT"
    
    columns_sql = []
    for col in df.columns:
        sql_type = map_dtype(df[col].dtype)
        columns_sql.append(f"`{col}` {sql_type}")
    
    create_table_sql = f"""
    CREATE TABLE `{table_name}` (
        {', '.join(columns_sql)}
    ) CHARACTER SET utf8mb4;
    """
    
    cursor.execute(create_table_sql)


def _insert_data(table_name, df, conn):
    """Insert data without IGNORE (for replace mode)."""
    cursor = conn.cursor()
    
    columns = ', '.join([f"`{col}`" for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))
    
    insert_sql = f"""
    INSERT INTO `{table_name}` ({columns})
    VALUES ({placeholders})
    """
    
    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))
    
    conn.commit()
    print(f"  ✅ {len(df)} rows 삽입 완료")


def _insert_ignore(table_name, df, conn):
    """Insert data with IGNORE (for append mode)."""
    cursor = conn.cursor()
    
    columns = ', '.join([f"`{col}`" for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))
    
    insert_sql = f"""
    INSERT IGNORE INTO `{table_name}` ({columns})
    VALUES ({placeholders})
    """
    
    total_rows = len(df)
    inserted_rows = 0
    
    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))
        inserted_rows += cursor.rowcount
    
    conn.commit()
    print(f"  ✅ 전체 {total_rows} rows, 중복 제외 후 삽입된 건: {inserted_rows} rows")
