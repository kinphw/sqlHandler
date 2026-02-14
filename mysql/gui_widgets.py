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
        
        # Source name (Dictionary key or Sheet name) - conditional
        self.widgets['lbl_source_name'] = tk.Label(self.lb_input_frame, text="소스 지정:")
        self.widgets['entry_source_name'] = tk.Entry(self.lb_input_frame, width=30)
        
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
            self.widgets['entry_source_name'].grid(row=2, column=1, sticky="w", padx=5, pady=5)
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
            self.widgets['entry_source_name'].grid_forget()
            self.widgets['lbl_source_help'].grid_forget()
            
            self.widgets['lbl_target_table'].grid_forget()
            self.widgets['entry_target_table'].grid_forget()

            self.widgets['lbl_table_collation_title'].grid_forget()
            self.widgets['lbl_table_collation_value'].grid_forget()
            self.widgets['lbl_collation_compare_title'].grid_forget()
            self.widgets['lbl_collation_compare_value'].grid_forget()
            
            # Note: We do NOT clear the variables here to strictly persist, 
            # or we can clear them if that's the desired UX. 
            # Original code cleared them.
            if 'entry_source_name' in self.widgets: self.widgets['entry_source_name'].delete(0, tk.END)
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
        
        source_name = self.widgets['entry_source_name'].get().strip()
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
