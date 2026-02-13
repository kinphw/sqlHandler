import pandas as pd
from sqlalchemy import create_engine

def export_to_pkl(db_url, table_name, output_path):
    """
    Exports a MySQL table to a Pickle file.
    
    Args:
        db_url (str): SQLAlchemy database URL.
        table_name (str): Name of the table to export.
        output_path (str): Path to save the Pickle file.
    """
    try:
        engine = create_engine(db_url)
        print(f"✅ [mysql2pkl] 데이터베이스 연결 성공!")
        
        print(f"▶ [mysql2pkl] 테이블 '{table_name}' 데이터 조회 중...")
        query = f"SELECT * FROM `{table_name}`"
        df = pd.read_sql(query, con=engine)
        print(f"✅ [mysql2pkl] 데이터 조회 완료: {df.shape[0]} rows, {df.shape[1]} columns")
        
        if df.empty:
            print("⚠️ [mysql2pkl] 조회된 데이터가 없습니다.")
            return False

        df.to_pickle(output_path)
        print(f"🎉 [mysql2pkl] Pickle 파일 저장 완료: {output_path}")
        return True

    except Exception as e:
        print(f"❌ [mysql2pkl] 오류 발생: {e}")
        raise e
