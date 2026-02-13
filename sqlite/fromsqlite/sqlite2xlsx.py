
import pandas as pd
import sqlite3
import os

def export_to_xlsx(db_path, export_scope, table_name=None, query=None, output_path=None):
    """
    Exports SQLite data to an Excel file.
    
    Args:
        db_path (str): Path to SQLite DB file.
        export_scope (str): 'table', 'database', or 'query'.
        table_name (str): Table name (for 'table' scope).
        query (str): Custom SQL query (for 'query' scope).
        output_path (str): Output Excel file path.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        print(f"✅ SQLite 연결 성공: {db_path}")
        
        if export_scope == "query":
            if not query:
                raise ValueError("Query required for query scope")
            
            print(f"▶ 사용자 정의 쿼리 실행 중...")
            df = pd.read_sql_query(query, conn)
            print(f"✅ 쿼리 실행 완료: {df.shape[0]} rows")
            
            df.to_excel(output_path, index=False)
            print(f"🎉 엑셀 저장 완료: {output_path}")
            
        elif export_scope == "table":
            if not table_name:
                raise ValueError("Table name required for table scope")
            
            print(f"▶ 테이블 '{table_name}' 조회 중...")
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            print(f"✅ 조회 완료: {df.shape[0]} rows")
            
            df.to_excel(output_path, index=False)
            print(f"🎉 엑셀 저장 완료: {output_path}")
            
        elif export_scope == "database":
            # Get all tables
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [r[0] for r in cursor.fetchall()]
            
            if not tables:
                print("⚠️ 데이터베이스에 테이블이 없습니다.")
                return False
                
            print(f"✅ 발견된 테이블: {len(tables)}개 ({', '.join(tables)})")
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for table in tables:
                    print(f"▶ 테이블 '{table}' 추출 중...")
                    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                    
                    sheet_name = table[:31]  # Excel limits
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"   ✓ {df.shape[0]} rows")
            
            print(f"🎉 전체 DB 엑셀 저장 완료: {output_path}")
            
        return True

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        raise e
    finally:
        if conn: conn.close()
