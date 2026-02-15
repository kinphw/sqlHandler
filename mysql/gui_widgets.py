import tkinter as tk
from tkinter import filedialog, ttk, scrolledtext, messagebox
import os

class MySQLView:
    def __init__(self, notebook, app_instance):
        self.app_instance = app_instance 
        self.tab = tk.Frame(notebook)
        
        self.widgets = {}
        self.paned_window = None
        self.right_panel = None
        self.is_query_panel_visible = False
        self.comparison_panel = None
        self.is_comparison_panel_visible = False
        self._comparison_col_vars = []  # list of (col_name, BooleanVar)
        self._comparison_on_confirm = None
        self._comparison_on_refresh = None
        
        # Initialize persistent variables here so they don't get overwritten/garbage collected
        self._init_variables()
        
        self._setup_ui()

    def _init_variables(self):
        # Persistent variables that need to keep their state/bindings across UI updates
        self.widgets['var_prod'] = tk.BooleanVar(value=True)
        self.widgets['var_mode'] = tk.StringVar(value="mysql2xlsx")

        # Export vars
        self.widgets['var_export_scope'] = tk.StringVar(value="table")

        # Import vars
        self.widgets['var_import_scope'] = tk.StringVar(value="all")
        self.widgets['var_source_name'] = tk.StringVar()
        self.widgets['var_target_table'] = tk.StringVar()
        self.widgets['var_import_mode'] = tk.StringVar(value="replace")
        self.widgets['var_collation'] = tk.StringVar(value="server_default")
        self.widgets['var_stop_on_mismatch'] = tk.BooleanVar(value=True)
        
    def _setup_ui(self):
        # --- Main Layout (Left: Settings/Log, Right: Query) ---
        self.paned_window = tk.PanedWindow(self.tab, orient=tk.HORIZONTAL)
        self.paned_window.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Left Panel (Settings & Log)
        left_panel = tk.Frame(self.paned_window)
        self.paned_window.add(left_panel, width=500)
        
        # Right Panel (Query Input) - Initially hidden
        self.right_panel = tk.Frame(self.paned_window)
        
        # --- DB Connection Section (Left Panel) ---
        lb_db_frame = tk.LabelFrame(left_panel, text="DB Connection Settings", padx=10, pady=10)
        lb_db_frame.pack(fill="x", padx=10, pady=5)

        tk.Label(lb_db_frame, text="DB Name:").grid(row=0, column=0, sticky="e")
        self.widgets['entry_db_name'] = tk.Entry(lb_db_frame)
        self.widgets['entry_db_name'].insert(0, os.getenv("MYSQL_DB", ""))
        self.widgets['entry_db_name'].grid(row=0, column=1, sticky="w", padx=5)

        self.widgets['chk_prod'] = tk.Checkbutton(lb_db_frame, text="Use Prod DB", variable=self.widgets['var_prod'])
        self.widgets['chk_prod'].grid(row=1, column=0, columnspan=2, sticky="w")
        
        self.widgets['lbl_db_info'] = tk.Label(lb_db_frame, text="", fg="gray")
        self.widgets['lbl_db_info'].grid(row=2, column=0, columnspan=2, sticky="w")

        # --- Mode Selection Section (Left Panel) ---
        lb_mode_frame = tk.LabelFrame(left_panel, text="Select Mode", padx=10, pady=10)
        lb_mode_frame.pack(fill="x", padx=10, pady=5)

        modes = [
            ("MySQL -> Excel", "mysql2xlsx"),
            ("MySQL -> Pickle", "mysql2pkl"),
            ("Excel -> MySQL", "xlsx2mysql"),
            ("Pickle -> MySQL", "pkl2mysql"),
        ]

        for text, value in modes:
            tk.Radiobutton(lb_mode_frame, text=text, variable=self.widgets['var_mode'], value=value).pack(anchor="w")

        # --- Dynamic Input Section (Left Panel) ---
        self.lb_input_frame = tk.LabelFrame(left_panel, text="Settings", padx=10, pady=10)
        self.lb_input_frame.pack(fill="x", padx=10, pady=5)

        # --- Log Output Section (Left Panel) ---
        lb_log_frame = tk.LabelFrame(left_panel, text="Log Output", padx=10, pady=10)
        lb_log_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.widgets['log_text'] = scrolledtext.ScrolledText(lb_log_frame, height=15, state='disabled', wrap='word')
        self.widgets['log_text'].pack(fill="both", expand=True)
        
        # --- Action Button (Left Panel) ---
        self.widgets['btn_run'] = tk.Button(left_panel, text="RUN", height=2, bg="#dddddd")
        self.widgets['btn_run'].pack(fill="x", padx=10, pady=10)

        # --- Query Input Section (Right Panel) ---
        lb_query_frame = tk.LabelFrame(self.right_panel, text="SQL Query Input (For Query Mode)", padx=10, pady=10)
        lb_query_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.widgets['txt_query'] = scrolledtext.ScrolledText(lb_query_frame, font=("Consolas", 10))
        self.widgets['txt_query'].pack(fill="both", expand=True)
        
        tk.Label(lb_query_frame, text="* Export 범위에서 '사용자 정의 쿼리' 선택 시 사용됩니다.", fg="gray").pack(anchor="w")

        # --- Comparison Panel (Right Panel for Import) - Initially hidden ---
        self.comparison_panel = tk.Frame(self.paned_window)

    def get_tab_frame(self):
        return self.tab

    def log(self, message):
        self.widgets['log_text'].configure(state='normal')
        self.widgets['log_text'].insert(tk.END, message + "\n")
        self.widgets['log_text'].see(tk.END)
        self.widgets['log_text'].configure(state='disabled')

    def toggle_query_panel(self, show):
        if show:
            if not self.is_query_panel_visible:
                self.paned_window.add(self.right_panel)
                self.is_query_panel_visible = True
        else:
            if self.is_query_panel_visible:
                self.paned_window.forget(self.right_panel)
                self.is_query_panel_visible = False

    def update_input_widgets(self, mode, on_query_mode_change=None):
        # Clear existing widgets in input frame
        for widget in self.lb_input_frame.winfo_children():
            widget.destroy() 
        
        if mode in ["mysql2xlsx", "mysql2pkl"]:
            self._create_export_widgets(mode, on_query_mode_change)
        elif mode in ["xlsx2mysql", "pkl2mysql"]:
            self._create_import_widgets(mode)

    def _create_export_widgets(self, mode, on_query_mode_change):
        # Export scope selection
        tk.Label(self.lb_input_frame, text="추출 범위:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        
        frame_scope = tk.Frame(self.lb_input_frame)
        frame_scope.grid(row=0, column=1, sticky="w", padx=5, pady=5, columnspan=2)
        
        tk.Radiobutton(frame_scope, text="특정 테이블", variable=self.widgets['var_export_scope'], 
                       value="table", command=lambda: self._toggle_export_entry(on_query_mode_change)).pack(side="left", padx=5)
        tk.Radiobutton(frame_scope, text="전체 데이터베이스", variable=self.widgets['var_export_scope'], 
                       value="database", command=lambda: self._toggle_export_entry(on_query_mode_change)).pack(side="left", padx=5)
        tk.Radiobutton(frame_scope, text="사용자 정의 쿼리", variable=self.widgets['var_export_scope'], 
                       value="query", command=lambda: self._toggle_export_entry(on_query_mode_change)).pack(side="left", padx=5)
        
        # Table name input
        tk.Label(self.lb_input_frame, text="테이블명:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        self.widgets['entry_table_name'] = tk.Entry(self.lb_input_frame, width=30)
        self.widgets['entry_table_name'].grid(row=1, column=1, sticky="w", padx=5, pady=5)
        
        # Initial state
        self._toggle_export_entry(on_query_mode_change)

    def _toggle_export_entry(self, on_query_mode_change):
        scope = self.widgets['var_export_scope'].get()
        if scope == "table":
            self.widgets['entry_table_name'].config(state="normal")
        else:
            self.widgets['entry_table_name'].delete(0, tk.END)
            self.widgets['entry_table_name'].config(state="disabled")
            
        if on_query_mode_change:
            on_query_mode_change(scope == "query")

    def _create_import_widgets(self, mode):
        self._on_file_selected = None  # callback set by controller
        # File path selection
        tk.Label(self.lb_input_frame, text="파일 경로:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        self.widgets['entry_file_path'] = tk.Entry(self.lb_input_frame, width=30)
        self.widgets['entry_file_path'].grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        self.widgets['btn_browse'] = tk.Button(self.lb_input_frame, text="Browse", 
                                          command=lambda: self._browse_file(mode))
        self.widgets['btn_browse'].grid(row=0, column=2, padx=5, pady=5)
        
        # Import scope selection
        tk.Label(self.lb_input_frame, text="Import 범위:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        
        frame_scope = tk.Frame(self.lb_input_frame)
        frame_scope.grid(row=1, column=1, sticky="w", padx=5, pady=5, columnspan=2)
        
        tk.Radiobutton(frame_scope, text="특정 테이블만", variable=self.widgets['var_import_scope'], 
                       value="single", command=self._toggle_import_scope_widgets).pack(side="left", padx=5)
        tk.Radiobutton(frame_scope, text="전체 (모든 키/시트)", variable=self.widgets['var_import_scope'], 
                       value="all", command=self._toggle_import_scope_widgets).pack(side="left", padx=5)
        
        # Source name (Dictionary key or Sheet name) - conditional, as Combobox
        self.widgets['lbl_source_name'] = tk.Label(self.lb_input_frame, text="소스 지정:")
        self.widgets['cmb_source_name'] = ttk.Combobox(
            self.lb_input_frame, textvariable=self.widgets['var_source_name'], width=27
        )

        # Help text for source
        help_text = "(Dictionary 키 또는 시트명)" if mode == "pkl2mysql" else "(시트명, 비워두면 첫 시트)"
        self.widgets['lbl_source_help'] = tk.Label(self.lb_input_frame, text=help_text, fg="gray", font=("", 8))
        
        # Target table name - conditional
        self.widgets['lbl_target_table'] = tk.Label(self.lb_input_frame, text="대상 테이블명:")
        
        # We reuse persistent var_target_table
        self.widgets['entry_target_table'] = tk.Entry(self.lb_input_frame, width=30, textvariable=self.widgets['var_target_table'])
        
        # Import mode selection
        tk.Label(self.lb_input_frame, text="Import 모드:").grid(row=5, column=0, sticky="e", padx=5, pady=5)
        
        frame_mode = tk.Frame(self.lb_input_frame)
        frame_mode.grid(row=5, column=1, sticky="w", padx=5, pady=5, columnspan=2)
        
        tk.Radiobutton(frame_mode, text="Replace (대체)", variable=self.widgets['var_import_mode'], 
                       value="replace").pack(side="left", padx=5)
        tk.Radiobutton(frame_mode, text="Append (추가)", variable=self.widgets['var_import_mode'], 
                       value="append").pack(side="left", padx=5)

        # Collation selection
        tk.Label(self.lb_input_frame, text="Collation:").grid(row=6, column=0, sticky="e", padx=5, pady=5)
        
        collations = [
            "server_default",
            "utf8mb4_uca1400_ai_ci",
            "utf8mb4_0900_ai_ci",
            "utf8mb4_unicode_ci",
            "utf8mb4_general_ci",
            "utf8mb4_bin",
        ]
        self.widgets['cmb_collation'] = ttk.Combobox(self.lb_input_frame, textvariable=self.widgets['var_collation'],
                                                values=collations, state="readonly", width=27)
        self.widgets['cmb_collation'].grid(row=6, column=1, sticky="w", padx=5, pady=5)
        self.widgets['lbl_collation_hint'] = tk.Label(self.lb_input_frame, text="(없음=DB 기본값)", fg="gray", font=("", 8))
        self.widgets['lbl_collation_hint'].grid(row=6, column=2, sticky="w", padx=5, pady=5)

        # Stop on collation mismatch
        self.widgets['chk_stop_on_mismatch'] = tk.Checkbutton(
            self.lb_input_frame,
            text="콜레이션 불일치 시 중단",
            variable=self.widgets['var_stop_on_mismatch']
        )
        self.widgets['chk_stop_on_mismatch'].grid(row=7, column=1, sticky="w", padx=5, pady=5)

        # Collation status (single table only)
        self.widgets['lbl_table_collation_title'] = tk.Label(self.lb_input_frame, text="테이블 콜레이션:")
        self.widgets['lbl_table_collation_value'] = tk.Label(self.lb_input_frame, text="-", fg="gray")
        self.widgets['lbl_collation_compare_title'] = tk.Label(self.lb_input_frame, text="비교 결과:")
        self.widgets['lbl_collation_compare_value'] = tk.Label(self.lb_input_frame, text="-", fg="gray")
        
        # Initial state
        self._toggle_import_scope_widgets()

    def _toggle_import_scope_widgets(self):
        import_scope = self.widgets['var_import_scope'].get()
        
        if import_scope == "single":
            # Show source and target widgets
            self.widgets['lbl_source_name'].grid(row=2, column=0, sticky="e", padx=5, pady=5)
            self.widgets['cmb_source_name'].grid(row=2, column=1, sticky="w", padx=5, pady=5)
            self.widgets['lbl_source_help'].grid(row=2, column=2, sticky="w", padx=5, pady=5)
            
            self.widgets['lbl_target_table'].grid(row=3, column=0, sticky="e", padx=5, pady=5)
            self.widgets['entry_target_table'].grid(row=3, column=1, sticky="w", padx=5, pady=5)

            self.widgets['lbl_table_collation_title'].grid(row=8, column=0, sticky="e", padx=5, pady=5)
            self.widgets['lbl_table_collation_value'].grid(row=8, column=1, sticky="w", padx=5, pady=5, columnspan=2)
            self.widgets['lbl_collation_compare_title'].grid(row=9, column=0, sticky="e", padx=5, pady=5)
            self.widgets['lbl_collation_compare_value'].grid(row=9, column=1, sticky="w", padx=5, pady=5, columnspan=2)
        else:
            # Hide source and target widgets
            self.widgets['lbl_source_name'].grid_forget()
            self.widgets['cmb_source_name'].grid_forget()
            self.widgets['lbl_source_help'].grid_forget()

            self.widgets['lbl_target_table'].grid_forget()
            self.widgets['entry_target_table'].grid_forget()

            self.widgets['lbl_table_collation_title'].grid_forget()
            self.widgets['lbl_table_collation_value'].grid_forget()
            self.widgets['lbl_collation_compare_title'].grid_forget()
            self.widgets['lbl_collation_compare_value'].grid_forget()

            self.widgets['var_source_name'].set("")
            self.widgets['var_target_table'].set("")

    def _browse_file(self, mode):
        filetypes = []
        if mode == "xlsx2xlsx":
            filetypes = [("Excel files", "*.xlsx *.xls")]
        elif mode == "pkl2mysql":
            filetypes = [("Pickle files", "*.pkl")]
        elif mode == "xlsx2mysql":
            filetypes = [("Excel files", "*.xlsx *.xls")]

        filepath = filedialog.askopenfilename(filetypes=filetypes)
        if filepath:
            self.widgets['entry_file_path'].delete(0, tk.END)
            self.widgets['entry_file_path'].insert(0, filepath)
            # Trigger source list update callback if registered
            if self._on_file_selected:
                self._on_file_selected(filepath, mode)

    # --- Public Accessor Methods for Controller ---

    def set_db_info_label(self, connection_str):
        self.widgets['lbl_db_info'].config(text=connection_str)

    def get_db_name(self):
        return self.widgets['entry_db_name'].get().strip()

    def get_prod_checked(self):
        return self.widgets['var_prod'].get()

    def get_mode(self):
        return self.widgets['var_mode'].get()

    def get_query_text(self):
        return self.widgets['txt_query'].get("1.0", tk.END).strip()

    def get_export_params(self):
        scope = self.widgets['var_export_scope'].get()
        if scope == "table":
            table_name = self.widgets['entry_table_name'].get().strip()
            if not table_name: return None
            return {'scope': 'table', 'table_name': table_name}
        elif scope == "database":
            return {'scope': 'database', 'table_name': None}
        else:
            return {'scope': 'query', 'table_name': None}

    def get_import_params(self):
        file_path = self.widgets['entry_file_path'].get().strip()
        if not file_path: return None

        source_name = self.widgets['var_source_name'].get().strip()
        target_table = self.widgets['entry_target_table'].get().strip()
        
        return {
            'file_path': file_path,
            'import_scope': self.widgets['var_import_scope'].get(),
            'source_name': source_name if source_name else None,
            'target_table': target_table if target_table else None,
            'if_exists': self.widgets['var_import_mode'].get(),
            'collation': self.widgets['var_collation'].get() or "server_default",
            'stop_on_mismatch': self.widgets['var_stop_on_mismatch'].get()
        }

    def get_target_table_info(self):
        # Lenient getter for collation check (doesn't require file path)
        return {
            'import_scope': self.widgets['var_import_scope'].get(),
            'target_table': self.widgets['var_target_table'].get().strip(),
            'collation': self.widgets['var_collation'].get() or "server_default"
        }

    def set_on_file_selected(self, callback):
        """Register callback for file selection: callback(filepath, mode)"""
        self._on_file_selected = callback

    def update_source_dropdown(self, values, help_text=None):
        """Update the source name combobox with given values."""
        if 'cmb_source_name' in self.widgets:
            self.widgets['cmb_source_name']['values'] = values
            if values:
                self.widgets['var_source_name'].set(values[0])
            else:
                self.widgets['var_source_name'].set("")
        if help_text and 'lbl_source_help' in self.widgets:
            self.widgets['lbl_source_help'].config(text=help_text)

    def update_collation_dropdown(self, values, current=None):
        if 'cmb_collation' in self.widgets:
            self.widgets['cmb_collation']['values'] = values
            if current:
                 self.widgets['var_collation'].set(current)

    def set_collation_hint(self, text):
         if 'lbl_collation_hint' in self.widgets:
             self.widgets['lbl_collation_hint'].config(text=text)
    
    def get_collation_current(self):
        return self.widgets['var_collation'].get()

    def set_collation_current(self, value):
        self.widgets['var_collation'].set(value)

    def set_table_collation_info(self, table_coll_text, compare_text, compare_color):
        if 'lbl_table_collation_value' in self.widgets:
             self.widgets['lbl_table_collation_value'].config(text=table_coll_text)
        if 'lbl_collation_compare_value' in self.widgets:
             self.widgets['lbl_collation_compare_value'].config(text=compare_text, fg=compare_color)

    def bind_event(self, key, handler):
        # Helper to bind events or commands to specific widgets
        if key == 'run_button':
            if 'btn_run' in self.widgets: self.widgets['btn_run'].config(command=handler)
        elif key == 'db_prod_change':
            if 'chk_prod' in self.widgets: self.widgets['chk_prod'].config(command=handler)
        elif key == 'mode_change':
             self.widgets['var_mode'].trace_add('write', handler)
        
        # Collation related triggers
        elif key == 'target_table_change':
            if 'var_target_table' in self.widgets:
                self.widgets['var_target_table'].trace_add('write', handler)
        elif key == 'collation_change':
            if 'var_collation' in self.widgets:
                self.widgets['var_collation'].trace_add('write', handler)
        elif key == 'import_scope_change':
             if 'var_import_scope' in self.widgets:
                self.widgets['var_import_scope'].trace_add('write', handler)
    
    def show_warning(self, title, message):
        messagebox.showwarning(title, message)
    
    def show_info(self, title, message):
        messagebox.showinfo(title, message)

    def show_error(self, title, message):
        messagebox.showerror(title, message)

    # --- Comparison Panel Methods ---

    def show_comparison_panel(self, table_name, df_columns, mysql_columns, table_index, total_tables, on_confirm, on_refresh):
        """
        Show the column comparison panel.

        Args:
            table_name: Target MySQL table name.
            df_columns: List of DataFrame column names (already normalized).
            mysql_columns: List of (col_name, data_type, col_key, extra) from MySQL, or None if new table.
            table_index: Current table index (0-based) for batch display.
            total_tables: Total number of tables to compare.
            on_confirm: Callback when user confirms.
            on_refresh: Callback to refresh comparison (re-fetch MySQL columns).
        """
        self._comparison_on_confirm = on_confirm
        self._comparison_on_refresh = on_refresh

        # Hide query panel if visible
        if self.is_query_panel_visible:
            self.toggle_query_panel(False)

        # Clear previous content
        for widget in self.comparison_panel.winfo_children():
            widget.destroy()
        self._comparison_col_vars = []

        # Build MySQL column lookup
        mysql_col_map = {}  # {col_name: (data_type, col_key, extra)}
        mysql_col_names = set()
        if mysql_columns:
            for col_name, data_type, col_key, extra in mysql_columns:
                mysql_col_map[col_name] = (data_type, col_key, extra)
                mysql_col_names.add(col_name)

        df_col_set = set(df_columns)

        # Header
        header_text = f"컬럼 비교: {table_name}"
        if total_tables > 1:
            header_text += f" ({table_index + 1}/{total_tables})"
        lb_frame = tk.LabelFrame(self.comparison_panel, text=header_text, padx=10, pady=10)
        lb_frame.pack(fill="both", expand=True, padx=10, pady=5)

        # Table status
        if mysql_columns:
            status_text = f"기존 테이블 (MySQL: {len(mysql_columns)}개 컬럼, DataFrame: {len(df_columns)}개 컬럼)"
        else:
            status_text = f"신규 테이블 (DataFrame: {len(df_columns)}개 컬럼)"
        tk.Label(lb_frame, text=status_text, fg="gray").pack(anchor="w", pady=(0, 5))

        # Two-column comparison body
        body = tk.Frame(lb_frame)
        body.pack(fill="both", expand=True)
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)

        # --- Left: DataFrame Columns (with checkboxes) ---
        left_label = tk.LabelFrame(body, text="DataFrame 컬럼 (체크 해제 시 제외)", padx=5, pady=5)
        left_label.grid(row=0, column=0, sticky="nsew", padx=(0, 5), pady=5)

        left_canvas = tk.Canvas(left_label, highlightthickness=0, height=400)
        left_scrollbar = ttk.Scrollbar(left_label, orient="vertical", command=left_canvas.yview)
        left_inner = tk.Frame(left_canvas)

        left_inner.bind("<Configure>", lambda e: left_canvas.configure(scrollregion=left_canvas.bbox("all")))
        left_canvas.create_window((0, 0), window=left_inner, anchor="nw")
        left_canvas.configure(yscrollcommand=left_scrollbar.set)

        left_canvas.pack(side="left", fill="both", expand=True)
        left_scrollbar.pack(side="right", fill="y")

        for col in df_columns:
            var = tk.BooleanVar(value=True)
            # Auto-uncheck auto_increment columns
            if col in mysql_col_map:
                _, _, extra = mysql_col_map[col]
                if 'auto_increment' in (extra or '').lower():
                    var.set(False)

            self._comparison_col_vars.append((col, var))

            # Determine color
            if col in mysql_col_names:
                fg_color = "#006400"  # dark green - exists in both
            else:
                fg_color = "#CC6600"  # orange - DataFrame only

            frame_row = tk.Frame(left_inner)
            frame_row.pack(anchor="w", fill="x")
            cb = tk.Checkbutton(frame_row, text=col, variable=var, fg=fg_color,
                                activeforeground=fg_color)
            cb.pack(side="left")
            # Show auto_increment hint
            if col in mysql_col_map:
                _, _, extra = mysql_col_map[col]
                if 'auto_increment' in (extra or '').lower():
                    tk.Label(frame_row, text="(auto_increment)", fg="gray",
                             font=("", 8)).pack(side="left")

        # --- Right: MySQL Table Columns ---
        right_label_text = "MySQL 테이블 컬럼" if mysql_columns else "MySQL 테이블 (신규 생성 예정)"
        right_label = tk.LabelFrame(body, text=right_label_text, padx=5, pady=5)
        right_label.grid(row=0, column=1, sticky="nsew", padx=(5, 0), pady=5)

        right_canvas = tk.Canvas(right_label, highlightthickness=0, height=400)
        right_scrollbar = ttk.Scrollbar(right_label, orient="vertical", command=right_canvas.yview)
        right_inner = tk.Frame(right_canvas)

        right_inner.bind("<Configure>", lambda e: right_canvas.configure(scrollregion=right_canvas.bbox("all")))
        right_canvas.create_window((0, 0), window=right_inner, anchor="nw")
        right_canvas.configure(yscrollcommand=right_scrollbar.set)

        right_canvas.pack(side="left", fill="both", expand=True)
        right_scrollbar.pack(side="right", fill="y")

        if mysql_columns:
            for col_name, data_type, col_key, extra in mysql_columns:
                if col_name in df_col_set:
                    fg_color = "#006400"  # green - matched
                else:
                    fg_color = "#999999"  # gray - MySQL only

                label_text = f"{col_name} ({data_type})"
                badges = []
                if col_key == "PRI":
                    badges.append("PK")
                if 'auto_increment' in (extra or '').lower():
                    badges.append("AI")
                if badges:
                    label_text += f" [{','.join(badges)}]"

                tk.Label(right_inner, text=label_text, fg=fg_color, anchor="w").pack(anchor="w")
        else:
            tk.Label(right_inner, text="테이블이 아직 존재하지 않습니다.\n컬럼 제외만 선택할 수 있습니다.",
                     fg="gray", justify="left").pack(anchor="w", pady=10)

        # --- Mismatch summary ---
        only_in_df = [c for c in df_columns if c not in mysql_col_names]
        only_in_mysql = [c for c in mysql_col_names if c not in df_col_set]

        summary_parts = []
        if only_in_df:
            summary_parts.append(f"DataFrame에만 {len(only_in_df)}개")
        if only_in_mysql:
            summary_parts.append(f"MySQL에만 {len(only_in_mysql)}개")

        if summary_parts and mysql_columns:
            summary_color = "#CC6600"
            summary_text = "컬럼 차이: " + ", ".join(summary_parts)
        elif not mysql_columns:
            summary_color = "gray"
            summary_text = "신규 테이블: 모든 컬럼이 새로 생성됩니다"
        else:
            summary_color = "#006400"
            summary_text = "모든 컬럼 일치"

        tk.Label(lb_frame, text=summary_text, fg=summary_color, font=("", 9, "bold")).pack(anchor="w", pady=(5, 0))

        # --- Buttons ---
        btn_frame = tk.Frame(lb_frame)
        btn_frame.pack(fill="x", pady=(10, 0))

        if on_refresh:
            tk.Button(btn_frame, text="Refresh", width=12,
                      command=self._on_comparison_refresh_clicked).pack(side="left", padx=5)

        if on_confirm:
            is_last = (table_index + 1 >= total_tables)
            confirm_text = "Confirm & Import" if is_last else "Next"
            tk.Button(btn_frame, text=confirm_text, width=16, bg="#4CAF50", fg="white",
                      command=self._on_comparison_confirm_clicked).pack(side="right", padx=5)
        else:
            tk.Label(btn_frame, text="* 컬럼 선택 후 RUN 버튼으로 Import 실행",
                     fg="gray", font=("", 8)).pack(side="right", padx=5)

        # Show panel
        if not self.is_comparison_panel_visible:
            self.paned_window.add(self.comparison_panel)
            self.is_comparison_panel_visible = True

    def _on_comparison_confirm_clicked(self):
        if self._comparison_on_confirm:
            self._comparison_on_confirm()

    def _on_comparison_refresh_clicked(self):
        if self._comparison_on_refresh:
            self._comparison_on_refresh()

    def hide_comparison_panel(self):
        if self.is_comparison_panel_visible:
            self.paned_window.forget(self.comparison_panel)
            self.is_comparison_panel_visible = False

    def get_excluded_columns(self):
        """Return list of column names where the checkbox is unchecked."""
        return [col_name for col_name, var in self._comparison_col_vars if not var.get()]
