import pandas as pd
from sqlalchemy import create_engine

def export_to_xlsx(db_url, table_name, output_path):
    """
    Exports a MySQL table to an Excel file.
    
    Args:
        db_url (str): SQLAlchemy database URL.
        table_name (str): Name of the table to export.
        output_path (str): Path to save the Excel file.
    """
    try:
        engine = create_engine(db_url)
        print(f"✅ [mysql2xlsx] 데이터베이스 연결 성공!")
        
        print(f"▶ [mysql2xlsx] 테이블 '{table_name}' 데이터 조회 중...")
        query = f"SELECT * FROM `{table_name}`"
        df = pd.read_sql(query, con=engine)
        print(f"✅ [mysql2xlsx] 데이터 조회 완료: {df.shape[0]} rows, {df.shape[1]} columns")
        
        if df.empty:
            print("⚠️ [mysql2xlsx] 조회된 데이터가 없습니다.")
            return False

        df.to_excel(output_path, index=False)
        print(f"🎉 [mysql2xlsx] 엑셀 파일 저장 완료: {output_path}")
        return True

    except Exception as e:
        print(f"❌ [mysql2xlsx] 오류 발생: {e}")
        raise e
