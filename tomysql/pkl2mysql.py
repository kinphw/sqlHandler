import pandas as pd
import pymysql
import os

def import_from_pkl(db_config, file_path, table_name):
    """
    Imports a Pickle file to MySQL.
    
    Args:
        db_config (dict): Dictionary with keys 'host', 'port', 'user', 'password', 'database'.
        file_path (str): Path to the Pickle file.
        table_name (str): Target table name.
    """
    conn = None
    try:
        # pkl2mysql uses pymysql directly for some parts, keeping logical consistency
        conn = pymysql.connect(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
        
        print(f"✅ [pkl2mysql] 데이터베이스 연결 성공!")
        
        # 2️⃣ 데이터프레임 불러오기
        df = pd.read_pickle(file_path)
        print(f"✅ [pkl2mysql] DataFrame 로딩 완료: {df.shape[0]} rows, {df.shape[1]} columns")

        # 3️⃣ 컬럼명 정제
        df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

        cursor = conn.cursor()

        # 5️⃣ 테이블 존재 여부 확인 후 없을 경우 생성
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()

        if not result:
            print(f"ℹ️ [pkl2mysql] 테이블 `{table_name}`이 존재하지 않아 생성합니다.")

            # dtype → MySQL 타입 매핑 함수
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
            conn.commit()
            print(f"🛠️ [pkl2mysql] 테이블 생성 완료: `{table_name}`")
        else:
            print(f"✅ [pkl2mysql] 테이블 `{table_name}`이 이미 존재합니다. 삭제 없이 유지합니다.")

        # 6️⃣ 데이터 업로드: INSERT IGNORE 유지
        _insert_ignore(table_name, df, conn)

        return True

    except Exception as e:
        print(f"❌ [pkl2mysql] 오류 발생: {e}")
        raise e
    finally:
        if conn:
            conn.close()

def _insert_ignore(table_name, df, conn):
    cursor = conn.cursor()

    columns = ', '.join([f"`{col}`" for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))

    insert_sql = f"""
    INSERT IGNORE INTO `{table_name}` ({columns})
    VALUES ({placeholders})
    """

    total_rows = len(df)
    inserted_rows = 0

    # id 컬럼 삭제 (DB의 AUTO_INCREMENT에 맡긴다)
    if 'id' in df.columns:
        df = df.drop(columns=['id'])

    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))
        inserted_rows += cursor.rowcount

    conn.commit()
    print(f"🎉 [pkl2mysql] 전체 건: {total_rows} rows, 중복 제외 후 삽입된 건: {inserted_rows} rows")
