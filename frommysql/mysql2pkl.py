import pandas as pd
from sqlalchemy import create_engine

def export_to_pkl(db_url, table_name, output_path):
    """
    Exports MySQL table(s) to a Pickle file.
    
    Args:
        db_url (str): SQLAlchemy database URL.
        table_name (str or None): Name of the table to export. If None, exports all tables as dictionary.
        output_path (str): Path to save the Pickle file.
    """
    try:
        engine = create_engine(db_url)
        print(f"✅ [mysql2pkl] 데이터베이스 연결 성공!")
        
        if table_name:
            # 특정 테이블만 추출
            print(f"▶ [mysql2pkl] 테이블 '{table_name}' 데이터 조회 중...")
            query = f"SELECT * FROM `{table_name}`"
            df = pd.read_sql(query, con=engine)
            print(f"✅ [mysql2pkl] 데이터 조회 완료: {df.shape[0]} rows, {df.shape[1]} columns")
            
            if df.empty:
                print("⚠️ [mysql2pkl] 조회된 데이터가 없습니다.")
                return False

            df.to_pickle(output_path)
            print(f"🎉 [mysql2pkl] Pickle 파일 저장 완료: {output_path}")
        else:
            # 전체 데이터베이스 추출 (딕셔너리 형태)
            print(f"▶ [mysql2pkl] 데이터베이스의 모든 테이블 조회 중...")
            tables_query = "SHOW TABLES"
            tables_df = pd.read_sql(tables_query, con=engine)
            table_list = tables_df.iloc[:, 0].tolist()
            
            if not table_list:
                print("⚠️ [mysql2pkl] 데이터베이스에 테이블이 없습니다.")
                return False
            
            print(f"✅ [mysql2pkl] {len(table_list)}개의 테이블 발견: {', '.join(table_list)}")
            
            # 딕셔너리 형태로 모든 테이블 저장
            all_tables = {}
            for table in table_list:
                print(f"▶ [mysql2pkl] 테이블 '{table}' 추출 중...")
                query = f"SELECT * FROM `{table}`"
                df = pd.read_sql(query, con=engine)
                all_tables[table] = df
                print(f"   ✅ {df.shape[0]} rows, {df.shape[1]} columns")
            
            # 딕셔너리를 pickle로 저장
            pd.to_pickle(all_tables, output_path)
            print(f"🎉 [mysql2pkl] 전체 데이터베이스 Pickle 파일 저장 완료: {output_path}")
            print(f"   💡 불러올 때: data = pd.read_pickle('{output_path}'); df = data['테이블명']")
        
        return True

    except Exception as e:
        print(f"❌ [mysql2pkl] 오류 발생: {e}")
        raise e
