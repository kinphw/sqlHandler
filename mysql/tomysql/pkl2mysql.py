import pandas as pd
from sqlalchemy import create_engine, inspect
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
    try:
        db_url = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{int(db_config['port'])}/{db_config['database']}?charset=utf8mb4"
        )
        engine = create_engine(db_url)
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
            
            # Import based on if_exists mode using pandas.to_sql
            _import_single_table(df, tbl_name, engine, if_exists)
            
            imported_count += 1
        
        scope_text = f"'{target_table}'" if import_scope == "single" else f"{imported_count}개 테이블"
        print(f"\n🎉 [pkl2mysql] {scope_text} Import 완료!")
        return True

    except Exception as e:
        print(f"❌ [pkl2mysql] 오류 발생: {e}")
        raise e
    finally:
        pass


def _import_single_table(df, table_name, engine, if_exists):
    """Import a single DataFrame to MySQL table using pandas.to_sql."""
    # Clean column names
    df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

    # _x000D_ 처리 (Excel 특수 문자와 동일하게 정리)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.replace('_x000D_', '', regex=False)

    mode_text = "대체" if if_exists == "replace" else "추가"

    # Check if table exists (for better messaging)
    inspector = inspect(engine)
    table_existed = table_name in inspector.get_table_names()

    if if_exists == "replace":
        if table_existed:
            print(f"  🗑️ 기존 테이블 '{table_name}' 삭제 후 재생성")
        else:
            print(f"  ℹ️ 테이블 '{table_name}' 신규 생성")
    else:
        if table_existed:
            print(f"  ✅ 기존 테이블 '{table_name}'에 데이터 추가")
        else:
            print(f"  ℹ️ 테이블 '{table_name}' 신규 생성 후 데이터 삽입")

    print(f"  ▶ Import 중 ({mode_text} 모드)...")
    df.to_sql(name=table_name, con=engine, index=False, if_exists=if_exists)
    print(f"  ✅ {len(df)} rows Import 완료")
