
"""
convert_db_to_base64.py
------------------------
SQLite 데이터베이스 파일을 Base64로 변환하여 JavaScript 파일로 저장하는 유틸리티
"""

import base64
import os
import sys

def convert_db_to_js(db_file, output_js=None):
    if not output_js:
        output_js = os.path.splitext(db_file)[0] + '.js'

    # SQLite DB 파일을 Base64로 변환
    with open(db_file, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")

    # JavaScript 파일 내용 구성 (Base64 + 복호화 함수 포함)
    js_content = f"""class Dataset {{
    databaseBase64 = "{encoded}";

    // Base64를 바이너리로 변환하여 Uint8Array 반환
    getDatabaseBinary() {{
        const binaryString = atob(this.databaseBase64);
        const bytes = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) {{
            bytes[i] = binaryString.charCodeAt(i);
        }}
        return bytes;
    }}
    }};
    
    window.Dataset = Dataset;
    """

    # JavaScript 파일로 저장
    with open(output_js, "w", encoding='utf-8') as f:
        f.write(js_content)
    
    print(f"✅ 변환 완료! {output_js} 파일이 생성되었습니다.")
    return output_js

if __name__ == "__main__":
    if len(sys.argv) > 1:
        convert_db_to_js(sys.argv[1])
    else:
        print("Usage: python convert_db_to_base64.py <db_file>")
