import tkinter as tk
from tkinter import ttk, scrolledtext


class CleanerView:
    _PREVIEW_CHAR_LIMIT = 160

    def __init__(self, notebook, app):
        self._frame = tk.Frame(notebook)
        self._app = app
        self._full_row_values = {}
        self._build_ui()

    def get_tab_frame(self):
        return self._frame

    def _build_ui(self):
        paned = tk.PanedWindow(self._frame, orient=tk.HORIZONTAL, sashwidth=5)
        paned.pack(fill="both", expand=True)

        left = tk.Frame(paned, width=270, bg="#f5f5f5")
        left.pack_propagate(False)
        paned.add(left, minsize=200)

        right = tk.Frame(paned)
        paned.add(right, minsize=400)

        self._build_left(left)
        self._build_right(right)

    # ------------------------------------------------------------------
    # Left panel: query settings + log
    # ------------------------------------------------------------------
    def _build_left(self, parent):
        # Connection status (read-only, from Connection tab)
        self.lbl_conn = tk.Label(
            parent, text="연결 없음", fg="gray",
            font=("Arial", 8), bg="#f5f5f5"
        )
        self.lbl_conn.pack(anchor="w", padx=10, pady=(10, 2))

        settings = tk.LabelFrame(parent, text="조회 설정", padx=8, pady=6)
        settings.pack(fill="x", padx=8, pady=(0, 4))

        fields = [
            ("테이블",  "entry_table"),
            ("WHERE",   "entry_where"),
            ("시작 ID", "entry_start_id"),
            ("끝 ID",   "entry_end_id"),
        ]
        for row, (label, attr) in enumerate(fields):
            tk.Label(settings, text=f"{label}:").grid(
                row=row, column=0, sticky="w", pady=2
            )
            entry = tk.Entry(settings, width=20)
            entry.grid(row=row, column=1, sticky="ew", padx=(6, 0), pady=2)
            setattr(self, attr, entry)

        tk.Label(
            settings, text="WHERE 절 이후 조건만 입력",
            fg="gray", font=("Arial", 7)
        ).grid(row=4, column=0, columnspan=2, sticky="w")

        tk.Label(
            settings, text="ID 범위는 감지된 키 컬럼 기준으로 적용",
            fg="gray", font=("Arial", 7)
        ).grid(row=5, column=0, columnspan=2, sticky="w")

        tk.Label(settings, text="LIMIT:").grid(
            row=6, column=0, sticky="w", pady=(6, 2)
        )
        self.entry_limit = tk.Entry(settings, width=8)
        self.entry_limit.insert(0, "500")
        self.entry_limit.grid(row=6, column=1, sticky="w", padx=(6, 0), pady=(6, 2))

        settings.columnconfigure(1, weight=1)

        self.lbl_id_col = tk.Label(
            parent, text="키 컬럼: -",
            fg="gray", font=("Arial", 8), bg="#f5f5f5"
        )
        self.lbl_id_col.pack(anchor="w", padx=10, pady=(2, 0))

        btn_frame = tk.Frame(parent, bg="#f5f5f5")
        btn_frame.pack(fill="x", padx=8, pady=6)

        self.btn_query = tk.Button(
            btn_frame, text="조회", width=10,
            bg="#4a90d9", fg="white", relief="flat", cursor="hand2"
        )
        self.btn_query.pack(side="left")

        self.btn_next = tk.Button(
            btn_frame, text="다음 500", width=10,
            relief="flat", cursor="hand2", state="disabled"
        )
        self.btn_next.pack(side="left", padx=(6, 0))

        self.lbl_count = tk.Label(parent, text="", fg="#555", bg="#f5f5f5")
        self.lbl_count.pack(anchor="w", padx=10)

        log_frame = tk.LabelFrame(parent, text="로그", padx=4, pady=4)
        log_frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=12, state="disabled",
            font=("Consolas", 8), wrap="word",
            bg="#1e1e1e", fg="#d4d4d4"
        )
        self.log_text.pack(fill="both", expand=True)

    # ------------------------------------------------------------------
    # Right panel: Treeview grid
    # ------------------------------------------------------------------
    def _build_right(self, parent):
        header = tk.Frame(parent)
        header.pack(fill="x", padx=6, pady=(10, 2))

        tk.Label(
            header, text="쿼리 결과", font=("Arial", 10, "bold")
        ).pack(side="left")
        tk.Label(
            header,
            text="  Del: 선택 삭제 | Ctrl+Click: 다중 선택 | Shift+Click: 범위 선택",
            fg="gray", font=("Arial", 8)
        ).pack(side="left")

        tree_frame = tk.Frame(parent)
        tree_frame.pack(fill="both", expand=True, padx=6, pady=(0, 8))

        self.tree = ttk.Treeview(tree_frame, selectmode="extended", show="headings")

        self._v_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self._h_scroll = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(
            yscrollcommand=self._v_scroll.set,
            xscrollcommand=self._h_scroll.set,
        )

        self._v_scroll.pack(side="right", fill="y")
        self._h_scroll.pack(side="bottom", fill="x")
        self.tree.pack(fill="both", expand=True)

    # ------------------------------------------------------------------
    # Public interface (used by controller)
    # ------------------------------------------------------------------
    def log(self, msg: str):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", msg + "\n")
        self.log_text.configure(state="disabled")
        self.log_text.see("end")

    def schedule(self, callback):
        """Run callback safely on the main thread."""
        self.tree.after(0, callback)

    def set_conn_label(self, text: str, color: str = "gray"):
        self.lbl_conn.config(text=text, fg=color)

    def set_columns(self, columns: list):
        self.tree["columns"] = columns
        for col in columns:
            self.tree.heading(col, text=col, anchor="w")
            self.tree.column(col, width=120, minwidth=60, anchor="w")

    def start_populate(self, rows: list, chunk_size: int = 200):
        """Insert rows in chunks so the event loop stays responsive."""
        tree = self.tree
        tree.configure(yscrollcommand="", xscrollcommand="")
        children = tree.get_children()
        if children:
            tree.delete(*children)

        self._pending = rows
        self._full_row_values = {}
        self._tk_call = tree.tk.call  # cache to avoid per-row attribute lookup
        self._w = tree._w
        self._chunk_size = chunk_size
        self._insert_chunk(0)

    def _format_preview_value(self, value):
        text = "" if value is None else str(value)
        text = " ".join(text.splitlines())
        if len(text) > self._PREVIEW_CHAR_LIMIT:
            return text[: self._PREVIEW_CHAR_LIMIT - 3] + "..."
        return text

    def _build_preview_row(self, row):
        return tuple(self._format_preview_value(value) for value in row)

    def _insert_chunk(self, start: int):
        rows = self._pending
        end = min(start + self._chunk_size, len(rows))
        tk_call, w = self._tk_call, self._w

        for i in range(start, end):
            iid = f"row_{i}"
            self._full_row_values[iid] = rows[i]
            tk_call(
                w,
                "insert",
                "",
                "end",
                "-id",
                iid,
                "-values",
                self._build_preview_row(rows[i]),
            )

        if end < len(rows):
            # Yield to event loop, then continue
            self.tree.after(0, lambda: self._insert_chunk(end))
        else:
            # All done — reattach scrollbars
            self.tree.configure(
                yscrollcommand=self._v_scroll.set,
                xscrollcommand=self._h_scroll.set,
            )
            self._pending = None

    def remove_rows(self, iids: tuple):
        for iid in iids:
            self._full_row_values.pop(iid, None)
            if self.tree.exists(iid):
                self.tree.delete(iid)

    def get_selected_iids(self) -> tuple:
        return self.tree.selection()

    def get_row_values(self, iid) -> tuple:
        return self._full_row_values.get(iid, self.tree.item(iid, "values"))

    def get_all_row_values(self) -> list:
        return [
            self._full_row_values.get(iid, self.tree.item(iid, "values"))
            for iid in self.tree.get_children()
        ]

    def set_count_label(self, count: int, id_col: str = None, first_id=None, last_id=None):
        text = f"현재 화면 {count:,}건"
        if id_col and first_id is not None and last_id is not None:
            text += f"  |  {id_col}: {first_id} ~ {last_id}"
        self.lbl_count.config(text=text)

    def set_id_col_label(self, col_name: str):
        self.lbl_id_col.config(text=f"키 컬럼: {col_name}", fg="#2a6496")

    def set_next_enabled(self, enabled: bool):
        self.btn_next.config(state="normal" if enabled else "disabled")

    def bind_event(self, event_name: str, callback):
        if event_name == "query":
            self.btn_query.config(command=callback)
        elif event_name == "next_page":
            self.btn_next.config(command=callback)
        elif event_name == "delete_key":
            self.tree.bind("<Delete>", callback)

    def get_table_name(self) -> str:
        return self.entry_table.get().strip()

    def get_where_clause(self) -> str:
        return self.entry_where.get().strip()

    def get_start_id(self) -> str:
        return self.entry_start_id.get().strip()

    def get_end_id(self) -> str:
        return self.entry_end_id.get().strip()

    def get_limit(self) -> int:
        try:
            return int(self.entry_limit.get().strip())
        except ValueError:
            return 500
