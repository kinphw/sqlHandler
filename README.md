# MySQL Data Handler

**Version:** 0.0.1

MySQL 데이터베이스와 Excel/Pickle 파일 간의 데이터 Import/Export를 지원하는 GUI 애플리케이션입니다.

## 주요 기능

### Export (MySQL → Excel/Pickle)
- **특정 테이블 추출**: 선택한 테이블을 Excel 또는 Pickle 파일로 저장
- **전체 DB 추출**: 데이터베이스의 모든 테이블을 하나의 파일로 저장
  - Excel: 각 테이블이 별도 시트로 저장
  - Pickle: Dictionary 형태로 저장 (키: 테이블명, 값: DataFrame)

### 미리보기 (Test 버튼)
- `Test (사전 체크)` 버튼은 실제 실행 없이 **별도 미리보기 창**을 띄운다 (좌측 표 + 우측 선택 행 상세).
  - **Export**: 특정 테이블/사용자 정의 쿼리는 상위 N행(기본 500)을 표로 미리보기. 전체 DB는 테이블별 행 수 요약만 표시.
  - **Import**: 시트/키별로 파일 데이터를 표로 보여주며, **행마다 기적재 검증 컬럼**을 함께 표시.
    - 검증 키는 **비교 패널 우측의 MySQL 컬럼에서 체크박스로 직접 선택**한다(복수 가능, Test 전용). 선택한 컬럼들의 값 조합이 DB에 같은 값으로 존재하면 기적재로 판정.
      - PK가 `auto_increment`(예: `id`)뿐이라 자동 인식이 무의미한 경우, `구분+제목+회신일자` 같은 **자연키**를 직접 골라 검증할 수 있다.
      - 파일에도 존재하는(매칭된) 컬럼만 체크할 수 있다.
    - `기적재`(빨강) = 선택 키 기준 이미 DB에 존재 / `신규`(초록) = 신규 적재 대상 / `확인불가`(회색) = 키 값에 NULL 포함
    - 키 미선택 시 검증 컬럼 없이 데이터만 표시. 여러 테이블('전체' 모드)은 테이블별 탭으로 표시
    - `auto_increment` 컬럼(예: `id`)은 import 시 새 값이 부여되므로 **검증 키 후보에서 제외**된다.
- 미리보기 표시 행 수는 상위 N행(기본 500)으로 제한되며, 상단 요약에는 전체 행 수와 (미리보기 기준) 기적재 건수가 함께 표시된다.
- **검증 키 재활용**: Test에서 찍은 검증 키는 저장되어, 이후 `RUN` 사전 체크의 중복 판정에도 그대로 재활용된다(PK 자동 인식보다 우선 — 자연키가 더 정확하므로). 요약에 `· 선택 키`로 표기.

### 사전 체크 (execute 전 자동 확인)
- **Export**: `RUN` 시 실제 추출 전에 대상 행 수를 먼저 조회하고 확인 다이얼로그를 표시
  - 특정 테이블: `SELECT COUNT(*)` 정확 행 수 (테이블이 없으면 경고 후 중단)
  - 전체 DB: 테이블별 행 수 + 총합
  - 사용자 정의 쿼리: 결과를 파생 테이블로 감싼 `COUNT(*)` (감쌀 수 없으면 진행 여부만 확인)
- **Import**: `RUN` 시 실제 Import 전에 파일 내용·DB 현황·중복을 요약하고 확인 다이얼로그를 표시
  - 시트/키별 **파일 행 수**
  - 대상 테이블의 **기존 DB 행 수** (없으면 "신규 테이블")
  - **중복 건수**: Test에서 찍은 검증 키가 있으면 그 키로(없으면 PK/UNIQUE 자동 인식, `auto_increment` 컬럼 제외) 파일의 고유 키가 DB에 이미 존재하는 건수
    - Append 모드: 해당 중복은 `INSERT IGNORE`로 건너뜀을 함께 안내
    - Replace 모드: 기존 데이터 삭제 후 교체임을 안내
- **콜레이션 / Append**: Append + 기존 테이블이면 콜레이션을 변경하지 않으므로, 콜레이션 불일치를 **참고용으로만 표시하고 import를 중단하지 않는다**("콜레이션 불일치 시 중단" 옵션과 무관). Replace/신규 생성에서만 불일치 중단이 적용된다.

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
- 대용량 데이터 Import 시 시간이 오래 걸릴 수 있습니다. (사전 체크 단계에서 파일을 한 번 더 읽으므로 매우 큰 파일은 RUN 직후 잠시 멈출 수 있습니다.)
- 중복 사전 체크는 대상 테이블에 PK 또는 UNIQUE 인덱스가 있고, 파일에 그 키 컬럼이 모두 존재할 때만 산출됩니다. 그 외에는 "키 없음" 또는 "중복 확인 불가"로 표시됩니다.
