"""데이터 미리보기 창 (Export/Import 실행 전 별도 뷰).

LawQuery-frc 의 gui/preview.py 패턴(Toplevel + 좌측 Treeview 목록 + 우측 상세)을
참고하되, 컬럼을 동적으로 받고 행 단위 검증결과(기적재/신규)를 함께 보여줄 수
있도록 범용화했다.

tables 인자 형식 (list of dict):
    {
        'name': str,                 # 탭/창 제목용 테이블명
        'columns': [str, ...],       # 표시 컬럼 (원본 컬럼명)
        'rows': [tuple, ...],        # 표시 행 (상위 N)
        'total_rows': int,           # 전체 행 수
        'status_per_row': [str,...] | None,  # 각 행 검증결과 ('기적재'/'신규'/'확인불가')
        'summary': str,              # 상단 요약 한 줄
    }
"""
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox


_MAX_CELL_LEN = 80


def _cell(value) -> str:
    """Treeview 셀 표시용: None/NaN→빈칸, 개행 제거, 길이 제한."""
    if value is None:
        return ""
    # NaN 검출 (float('nan') != 자기 자신)
    if isinstance(value, float) and value != value:
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    if len(text) > _MAX_CELL_LEN:
        text = text[:_MAX_CELL_LEN] + "…"
    return text


def _full(value) -> str:
    """상세 패널 표시용: None/NaN→빈칸, 그 외 전체 문자열."""
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    return str(value)


def open_data_preview(parent, window_title, tables):
    """미리보기 창을 띄운다. 테이블이 여러 개면 Notebook 탭으로 표시."""
    tables = [t for t in tables if t]
    if not tables:
        messagebox.showinfo("미리보기", "표시할 데이터가 없습니다.", parent=parent)
        return

    win = tk.Toplevel(parent)
    win.title(window_title)
    win.geometry("1400x850")
    win.minsize(1000, 600)
    win.transient(parent.winfo_toplevel())

    if len(tables) == 1:
        _build_table_view(win, tables[0])
    else:
        notebook = ttk.Notebook(win)
        notebook.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        for t in tables:
            frame = ttk.Frame(notebook)
            notebook.add(frame, text=t.get('name', '(table)'))
            _build_table_view(frame, t)

    return win


def _build_table_view(container, t):
    columns = list(t.get('columns', []))
    rows = t.get('rows', [])
    status = t.get('status_per_row')
    has_status = status is not None

    # --- Top summary ---
    summary = t.get('summary') or ""
    if has_status:
        summary += "    [검증] 기적재=이미 DB에 존재 / 신규=신규 적재 대상"
    tk.Label(container, text=summary, anchor="w", justify="left",
             fg="#333", font=("Malgun Gothic", 9)).pack(fill=tk.X, padx=10, pady=(8, 4))

    paned = ttk.Panedwindow(container, orient=tk.HORIZONTAL)
    paned.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

    left = ttk.Frame(paned)
    right = ttk.Frame(paned)
    paned.add(left, weight=3)
    paned.add(right, weight=2)

    # --- Left: Treeview grid ---
    display_cols = (["검증"] if has_status else []) + columns
    tree = ttk.Treeview(left, columns=display_cols, show="headings")

    vsb = ttk.Scrollbar(left, orient=tk.VERTICAL, command=tree.yview)
    hsb = ttk.Scrollbar(left, orient=tk.HORIZONTAL, command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    left.rowconfigure(0, weight=1)
    left.columnconfigure(0, weight=1)

    for c in display_cols:
        tree.heading(c, text=c)
        width = 80 if c == "검증" else 150
        tree.column(c, width=width, anchor="w", stretch=False)

    tree.tag_configure("exist", background="#ffecec", foreground="#b00000")
    tree.tag_configure("new", background="#eaffea", foreground="#006400")
    tree.tag_configure("unknown", background="#f4f4f4", foreground="#777777")

    for i, row in enumerate(rows):
        cells = [_cell(v) for v in row]
        if has_status:
            st = status[i]
            tag = "exist" if st == "기적재" else ("new" if st == "신규" else "unknown")
            tree.insert("", tk.END, iid=str(i), values=[st] + cells, tags=(tag,))
        else:
            tree.insert("", tk.END, iid=str(i), values=cells)

    # --- Right: detail of selected row ---
    detail_frame = ttk.LabelFrame(right, text="선택 행 상세")
    detail_frame.pack(fill=tk.BOTH, expand=True)
    detail = scrolledtext.ScrolledText(detail_frame, wrap=tk.WORD,
                                       font=("Malgun Gothic", 10), state=tk.DISABLED)
    detail.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

    def render(idx):
        row = rows[idx]
        lines = []
        if has_status:
            lines.append(f"[검증] {status[idx]}")
            lines.append("")
        for col, value in zip(columns, row):
            lines.append(f"● {col}")
            lines.append(_full(value))
            lines.append("")
        detail.configure(state=tk.NORMAL)
        detail.delete("1.0", tk.END)
        detail.insert("1.0", "\n".join(lines))
        detail.configure(state=tk.DISABLED)

    def on_select(_event=None):
        selection = tree.selection()
        if selection:
            render(int(selection[0]))

    tree.bind("<<TreeviewSelect>>", on_select)

    first = tree.get_children()
    if first:
        tree.selection_set(first[0])
        tree.focus(first[0])
        render(0)
