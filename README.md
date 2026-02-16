# MySQL Data Handler

**Version:** 0.0.1

MySQL 데이터베이스와 Excel/Pickle 파일 간의 데이터 Import/Export를 지원하는 GUI 애플리케이션입니다.

## 주요 기능

### Export (MySQL → Excel/Pickle)
- **특정 테이블 추출**: 선택한 테이블을 Excel 또는 Pickle 파일로 저장
- **전체 DB 추출**: 데이터베이스의 모든 테이블을 하나의 파일로 저장
  - Excel: 각 테이블이 별도 시트로 저장
  - Pickle: Dictionary 형태로 저장 (키: 테이블명, 값: DataFrame)

### Import (Excel/Pickle → MySQL)
- **특정 테이블 Import**: 파일의 특정 시트/키만 선택하여 Import
  - 소스(시트명/키)와 타겟(테이블명) 분리 지정 가능
- **전체 Import**: 파일의 모든 시트/키를 한 번에 Import
- **Import 모드 선택**:
  - **Replace (대체)**: 기존 테이블 삭제 후 재생성
  - **Append (추가)**: 기존 테이블에 데이터 추가 (중복 제외)

## 설치

### 필수 요구사항
- Python 3.8 이상
- MySQL 서버

### 의존성 설치
```bash
pip install pandas openpyxl sqlalchemy pymysql python-dotenv
```

## 설정

프로젝트 루트 디렉토리에 `.env` 파일을 생성하고 데이터베이스 연결 정보를 입력합니다:

```env
# 개발 환경
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=your_username
MYSQL_PASSWORD=your_password
MYSQL_DB=your_database

# 프로덕션 환경 (선택사항)
PROD_MYSQL_HOST=prod_host
PROD_MYSQL_PORT=3306
PROD_MYSQL_USER=prod_username
PROD_MYSQL_PASSWORD=prod_password
PROD_MYSQL_DB=prod_database
```

## 사용법

### 애플리케이션 실행
- **방법 1**: `main.pyw` 파일을 더블 클릭하여 실행 (콘솔 창 없음)
- **방법 2**: 터미널에서 실행
  ```bash
  python main.pyw
  ```

### Export 예시

#### 1. 특정 테이블 추출
1. 모드 선택: `MySQL → Excel` 또는 `MySQL → Pickle`
2. DB 환경 선택: `DEV` 또는 `PROD` 
3. Export 범위: **특정 테이블** 선택
4. 테이블명 입력: `users`
5. `Run` 버튼 클릭
6. 저장 위치 선택

#### 2. 전체 DB 추출
1. 모드 선택: `MySQL → Excel` 또는 `MySQL → Pickle`
2. DB 환경 선택: `DEV` 또는 `PROD`
3. Export 범위: **전체 데이터베이스** 선택
4. `Run` 버튼 클릭
5. 저장 위치 선택

### Import 예시

#### 1. 특정 시트/키만 Import
1. 모드 선택: `Excel → MySQL` 또는 `Pickle → MySQL`
2. DB 환경 선택: `DEV` 또는 `PROD`
3. Import 범위: **특정 테이블만** 선택
4. 파일 경로: `Browse` 버튼으로 파일 선택
5. 소스 지정: 시트명 또는 Dictionary 키 입력 (예: `Sheet1` 또는 `users`)
6. 대상 테이블명: MySQL 테이블명 입력 (예: `users_backup`)
7. Import 모드: `Replace` 또는 `Append` 선택
8. `Run` 버튼 클릭

#### 2. 전체 파일 Import
1. 모드 선택: `Excel → MySQL` 또는 `Pickle → MySQL`
2. DB 환경 선택: `DEV` 또는 `PROD`
3. Import 범위: **전체 (모든 키/시트)** 선택
4. 파일 경로: `Browse` 버튼으로 파일 선택
5. Import 모드: `Replace` 또는 `Append` 선택
6. `Run` 버튼 클릭

## 프로젝트 구조

```
sqlHandler/
├── main.pyw                   # GUI 애플리케이션 진입점 (콘솔 없음)
├── frommysql/                 # MySQL → Excel/Pickle
│   ├── mysql2xlsx.py         # Excel Export 로직
│   ├── mysql2pkl.py          # Pickle Export 로직
│   └── gui_widgets.py        # Export GUI 위젯
├── tomysql/                   # Excel/Pickle → MySQL
│   ├── xlsx2mysql.py         # Excel Import 로직
│   ├── pkl2mysql.py          # Pickle Import 로직
│   └── gui_widgets.py        # Import GUI 위젯
├── .env                       # DB 연결 설정 (직접 생성 필요)
├── .gitignore
└── README.md
```

## 유사 프로젝트 (GitHub 검색 결과)

아래는 GitHub 공개 저장소에서 "MySQL ↔ Excel import/export" 키워드로 검색한 유사 프로젝트입니다.
기능 범위나 사용 언어가 다를 수 있으니, 필요에 맞게 비교해보세요. (검색일: 2026-02-13)

- [merofeev/mysql2xlsx](https://github.com/merofeev/mysql2xlsx): MySQL 전체 DB를 Excel(xlsx)로 내보내는 파이썬 스크립트
- [BOOMER74/excel_mysql](https://github.com/BOOMER74/excel_mysql): Excel ↔ MySQL import/export 모듈(PHP, PHPExcel 기반)
- [fccn/nau-database-exporter](https://github.com/fccn/nau-database-exporter): Open edX MySQL 데이터를 Excel/Google Sheet로 내보내는 파이썬 도구
- [junffzhou/excel_import_export_tool](https://github.com/junffzhou/excel_import_export_tool): Golang 기반 MySQL Excel import/export 도구

## 라이선스

MIT License

## 주의사항

- **Replace 모드**는 기존 데이터를 완전히 삭제합니다. 중요한 데이터는 백업 후 사용하세요.
- **Append 모드**는 `INSERT IGNORE`를 사용하여 중복 데이터를 자동으로 제외합니다.
- 대용량 데이터 Import 시 시간이 오래 걸릴 수 있습니다.
