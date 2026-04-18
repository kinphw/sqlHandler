import pandas as pd
from sqlalchemy import create_engine, text
from mysql.services.query_safety import validate_read_only_query

def export_to_pkl(db_url, export_scope, table_name=None, query=None, output_path=None):
    """
    Exports MySQL table(s) to a Pickle file.
    
    Args:
        db_url (str): SQLAlchemy database URL.
        export_scope (str): 'table', 'database', or 'query'.
        table_name (str or None): Name of the table to export.
        query (str or None): Custom SQL query (for 'query' scope).
        output_path (str): Path to save the Pickle file.
    """
    engine = None
    try:
        engine = create_engine(db_url)
        print(f"✅ [mysql2pkl] 데이터베이스 연결 성공!")
        
        if export_scope == "query":
            # 사용자 정의 쿼리 실행
            if not query:
                raise ValueError("쿼리 스코프를 선택했을 경우, 'query' 인자는 필수입니다.")

            validate_read_only_query(query)

            print(f"▶ [mysql2pkl] 사용자 정의 쿼리 실행 중...")
            df = pd.read_sql(text(query), con=engine)
            print(f"✅ [mysql2pkl] 쿼리 실행 완료: {df.shape[0]} rows, {df.shape[1]} columns")
            
            df.to_pickle(output_path)
            print(f"🎉 [mysql2pkl] Pickle 파일 저장 완료: {output_path}")

        elif export_scope == "table":
            if not table_name:
                raise ValueError("테이블 스코프를 선택했을 경우, 'table_name' 인자는 필수입니다.")

            # 특정 테이블만 추출
            print(f"▶ [mysql2pkl] 테이블 '{table_name}' 데이터 조회 중...")
            safe_table_name = table_name.replace("`", "``")
            df = pd.read_sql(text(f"SELECT * FROM `{safe_table_name}`"), con=engine)
            print(f"✅ [mysql2pkl] 데이터 조회 완료: {df.shape[0]} rows, {df.shape[1]} columns")
            
            if df.empty:
                print("⚠️ [mysql2pkl] 조회된 데이터가 없습니다.")
                return False

            df.to_pickle(output_path)
            print(f"🎉 [mysql2pkl] Pickle 파일 저장 완료: {output_path}")

        elif export_scope == "database":
            # 전체 데이터베이스 추출 (딕셔너리 형태)
            print(f"▶ [mysql2pkl] 데이터베이스의 모든 테이블 조회 중...")
            tables_query = text("SHOW TABLES")
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
                df = pd.read_sql(text(f"SELECT * FROM `{table}`"), con=engine)
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
    finally:
        if engine:
            engine.dispose()
