import pandas as pd
from sqlalchemy import create_engine
import os

def import_from_xlsx(db_url, file_path, import_scope="all", source_name=None, target_table=None, if_exists="replace"):
    """
    Imports an Excel file to MySQL. Supports both single sheet and full import.
    
    Args:
        db_url (str): SQLAlchemy database URL.
        file_path (str): Path to the Excel file.
        import_scope (str): 'single' for specific sheet, 'all' for full import.
        source_name (str, optional): Sheet name to import (for single mode, None for first sheet).
        target_table (str, optional): Target table name (for single mode).
        if_exists (str): 'replace' to drop existing table, 'append' to add to existing table.
    """
    try:
        engine = create_engine(db_url)
        print(f"✅ [xlsx2mysql] 데이터베이스 연결 성공!")
        
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
            
            # Import single table
            _import_single_table(df, target_table, engine, if_exists)
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
                
                _import_single_table(df, table_name, engine, if_exists)
            
            print(f"\n🎉 [xlsx2mysql] 총 {len(sheets)}개 테이블 Import 완료!")

        return True

    except Exception as e:
        print(f"❌ [xlsx2mysql] 오류 발생: {e}")
        raise e


def _import_single_table(df, table_name, engine, if_exists):
    """Import a single DataFrame to MySQL table."""
    # _x000D_ 처리 (Excel 특수 문자)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.replace('_x000D_', '', regex=False)
    
    mode_text = "대체" if if_exists == "replace" else "추가"
    
    # Check if table exists (for better messaging)
    from sqlalchemy import inspect
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
