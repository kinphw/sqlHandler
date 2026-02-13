
"""
wasmWithBase64.py
------------------------
WASM 파일을 Base64로 변환하여 JavaScript 파일로 저장하는 스크립트
"""

import base64
import sys
import os

def convert_wasm_to_js(wasm_input_path, js_output_path=None):
    if not js_output_path:
        js_output_path = os.path.splitext(wasm_input_path)[0] + '-b64.js'
        
    # 1) sql-wasm.wasm 파일을 이진 모드로 읽음
    with open(wasm_input_path, "rb") as f:
        wasm_data = f.read()

    # 2) base64로 인코딩
    encoded_str = base64.b64encode(wasm_data).decode('utf-8')

    # 3) JS 파일 생성
    with open(js_output_path, "w", encoding="utf-8") as js_out:
        js_out.write(f"// Generated from {os.path.basename(wasm_input_path)}\n\n")
        js_out.write("window.WASM_BASE64 = `\n")
        js_out.write(encoded_str)
        js_out.write("\n`;")

    print(f"완료: {wasm_input_path} -> {js_output_path}")
    return js_output_path

if __name__ == "__main__":
    if len(sys.argv) > 1:
        convert_wasm_to_js(sys.argv[1])
    else:
        print("Usage: python wasmWithBase64.py <wasm_file>")
