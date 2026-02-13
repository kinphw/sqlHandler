import pandas as pd
from sqlalchemy import create_engine
import os

def import_from_xlsx(db_url, file_path, table_name=None):
    """
    Imports an Excel file to MySQL.
    
    Args:
        db_url (str): SQLAlchemy database URL.
        file_path (str): Path to the Excel file.
        table_name (str, optional): Target table name. If None, uses sheet names.
    """
    try:
        engine = create_engine(db_url)
        print(f"✅ [xlsx2mysql] 데이터베이스 연결 성공!")
        
        if table_name:
            # 특정 테이블 이름이 지정된 경우: 첫 번째 시트만 해당 테이블로 업로드
            print(f"▶ [xlsx2mysql] 엑셀 파일 '{os.path.basename(file_path)}' 읽는 중...")
            df = pd.read_excel(file_path, sheet_name=0) # 첫 번째 시트
            
            print(f"▶ [xlsx2mysql] 테이블 '{table_name}'로 업로드 중 (Rows: {len(df)})...")
            
            # _x000D_ 처리
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str).str.replace('_x000D_', '', regex=False)
                
            df.to_sql(name=table_name, con=engine, index=False, if_exists="replace")
            print(f"✅ [xlsx2mysql] 테이블 '{table_name}' 업로드 완료!")
            
        else:
            # 테이블 이름이 지정되지 않은 경우: 모든 시트를 각각의 테이블로 업로드
            print(f"▶ [xlsx2mysql] 엑셀 파일 '{os.path.basename(file_path)}'의 모든 시트 읽는 중...")
            sheets = pd.read_excel(file_path, sheet_name=None)
            
            for sheet_name, df in sheets.items():
                current_table_name = sheet_name.strip().lower().replace(" ", "_")
                print(f"▶ [xlsx2mysql] 시트 '{sheet_name}' → 테이블 '{current_table_name}' 업로드 중 (Rows: {len(df)})...")

                # _x000D_ 처리
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str).str.replace('_x000D_', '', regex=False)

                df.to_sql(name=current_table_name, con=engine, index=False, if_exists="replace")
                print(f"✅ [xlsx2mysql] 테이블 '{current_table_name}' 업로드 완료!")

        return True

    except Exception as e:
        print(f"❌ [xlsx2mysql] 오류 발생: {e}")
        raise e
