import pandas as pd
from sqlalchemy import create_engine

def export_to_xlsx(db_url, export_scope, table_name=None, query=None, output_path=None):
    """
    Exports MySQL data to an Excel file.
    
    Args:
        db_url (str): SQLAlchemy database URL.
        export_scope (str): 'table', 'database', or 'query'.
        table_name (str, optional): Name of the table to export (for 'table' scope).
        query (str, optional): Custom SQL query (for 'query' scope).
        output_path (str): Path to save the Excel file.
    """
    try:
        engine = create_engine(db_url)
        print(f"✅ [mysql2xlsx] 데이터베이스 연결 성공!")
        
        if export_scope == "query":
            # 사용자 정의 쿼리 실행
            if not query:
                raise ValueError("쿼리 스코프를 선택했을 경우, 'query' 인자는 필수입니다.")
            if not output_path:
                raise ValueError("쿼리 스코프를 선택했을 경우, 'output_path' 인자는 필수입니다.")

            print(f"▶ [mysql2xlsx] 사용자 정의 쿼리 실행 중...")
            
            # % 문자 처리: pd.read_sql은 params 인자가 없으면 %를 포맷팅 문자로 처리하지 않지만,
            # 만약 내부적으로 처리 과정에서 문제가 된다면 sqlalchemy text()를 사용하는 것이 안전함.
            # 하지만 여기서는 단순 실행이므로, 사용자가 입력한 쿼리 그대로 실행되도록 함.
            # 에러 메시지 "unsupported format character"는 f-string이나 % 포맷팅에서 발생함.
            # 코드 상에서 f-string 내부에 query 변수를 직접 넣지는 않았으므로, 
            # pd.read_sql 내부나 다른 라이브러리(pymysql/sqlalchemy) 연동 과정에서의 이슈일 가능성 높음.
            # 가장 확실한 해결책은 sqlalchemy의 text() 객체로 감싸는 것.
            
            from sqlalchemy import text
            df = pd.read_sql(text(query), con=engine)
            print(f"✅ [mysql2xlsx] 쿼리 실행 완료: {df.shape[0]} rows, {df.shape[1]} columns")
            
            if df.empty:
                print("⚠️ [mysql2xlsx] 조회된 데이터가 없습니다.")
                return False

            df.to_excel(output_path, index=False)
            print(f"🎉 [mysql2xlsx] 쿼리 결과 엑셀 파일 저장 완료: {output_path}")

        elif export_scope == "table":
            # 특정 테이블만 추출
            if not table_name:
                raise ValueError("테이블 스코프를 선택했을 경우, 'table_name' 인자는 필수입니다.")
            if not output_path:
                raise ValueError("테이블 스코프를 선택했을 경우, 'output_path' 인자는 필수입니다.")
                
            print(f"▶ [mysql2xlsx] 테이블 '{table_name}' 데이터 조회 중...")
            table_query = f"SELECT * FROM `{table_name}`"
            df = pd.read_sql(table_query, con=engine)
            print(f"✅ [mysql2xlsx] 데이터 조회 완료: {df.shape[0]} rows, {df.shape[1]} columns")
            
            if df.empty:
                print("⚠️ [mysql2xlsx] 조회된 데이터가 없습니다.")
                return False

            df.to_excel(output_path, index=False)
            print(f"🎉 [mysql2xlsx] 엑셀 파일 저장 완료: {output_path}")

        elif export_scope == "database":
            # 전체 데이터베이스 추출
            if not output_path:
                raise ValueError("데이터베이스 스코프를 선택했을 경우, 'output_path' 인자는 필수입니다.")

            print(f"▶ [mysql2xlsx] 데이터베이스의 모든 테이블 조회 중...")
            tables_query = "SHOW TABLES"
            tables_df = pd.read_sql(tables_query, con=engine)
            table_list = tables_df.iloc[:, 0].tolist()
            
            if not table_list:
                print("⚠️ [mysql2xlsx] 데이터베이스에 테이블이 없습니다.")
                return False
            
            print(f"✅ [mysql2xlsx] {len(table_list)}개의 테이블 발견: {', '.join(table_list)}")
            
            # ExcelWriter로 여러 시트 작성
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for table in table_list:
                    print(f"▶ [mysql2xlsx] 테이블 '{table}' 추출 중...")
                    query = f"SELECT * FROM `{table}`"
                    df = pd.read_sql(query, con=engine)
                    
                    # 시트 이름은 31자로 제한 (Excel 제약)
                    sheet_name = table[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"   ✅ {df.shape[0]} rows, {df.shape[1]} columns")
            
            print(f"🎉 [mysql2xlsx] 전체 데이터베이스 엑셀 파일 저장 완료: {output_path}")
        
        return True

    except Exception as e:
        print(f"❌ [mysql2xlsx] 오류 발생: {e}")
        raise e
