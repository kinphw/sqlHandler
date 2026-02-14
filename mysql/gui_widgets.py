import tkinter as tk
from tkinter import filedialog, ttk

def create_export_widgets(parent_frame, mode, on_query_mode_change=None):
    """
    Creates GUI widgets for export operations (MySQL -> Excel/Pickle).
    
    Args:
        parent_frame: Tkinter frame to add widgets to
        mode: 'mysql2xlsx' or 'mysql2pkl'
        on_query_mode_change: Callback function(is_query_mode)
    
    Returns:
        dict: Dictionary containing widget references
    """
    # Clear existing widgets
    for widget in parent_frame.winfo_children():
        widget.grid_forget()
    
    widgets = {}
    
    # Export scope selection
    widgets['var_export_scope'] = tk.StringVar(value="table")
    
    tk.Label(parent_frame, text="추출 범위:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
    
    frame_scope = tk.Frame(parent_frame)
    frame_scope.grid(row=0, column=1, sticky="w", padx=5, pady=5, columnspan=2)
    
    tk.Radiobutton(frame_scope, text="특정 테이블", variable=widgets['var_export_scope'], 
                   value="table", command=lambda: toggle_entry()).pack(side="left", padx=5)
    tk.Radiobutton(frame_scope, text="전체 데이터베이스", variable=widgets['var_export_scope'], 
                   value="database", command=lambda: toggle_entry()).pack(side="left", padx=5)
    tk.Radiobutton(frame_scope, text="사용자 정의 쿼리", variable=widgets['var_export_scope'], 
                   value="query", command=lambda: toggle_entry()).pack(side="left", padx=5)
    
    # Table name input
    tk.Label(parent_frame, text="테이블명:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
    widgets['entry_table_name'] = tk.Entry(parent_frame, width=30)
    widgets['entry_table_name'].grid(row=1, column=1, sticky="w", padx=5, pady=5)
    
    def toggle_entry():
        scope = widgets['var_export_scope'].get()
        if scope == "table":
            widgets['entry_table_name'].config(state="normal")
        else:
            widgets['entry_table_name'].delete(0, tk.END)
            widgets['entry_table_name'].config(state="disabled")
            
        # Callback for main window to show/hide query panel
        if on_query_mode_change:
            on_query_mode_change(scope == "query")
    
    # Initial state
    toggle_entry()
    
    return widgets


def get_export_params(widgets):
    """
    Extracts export parameters from widgets.
    
    Args:
        widgets: Dictionary of widget references from create_export_widgets
    
    Returns:
        dict: {'table_name': str or None}
    """
    export_scope = widgets['var_export_scope'].get()
    
    if export_scope == "table":
        table_name = widgets['entry_table_name'].get().strip()
        if not table_name:
            return None  # Validation failed
        return {'scope': 'table', 'table_name': table_name}
    elif export_scope == "database":
        # Full database export
        return {'scope': 'database', 'table_name': None}
    else:
        # Custom query export
        return {'scope': 'query', 'table_name': None}

# --- Import Widgets (from tomysql) ---

def create_import_widgets(parent_frame, mode):
    """
    Creates GUI widgets for import operations (Excel/Pickle -> MySQL).
    
    Args:
        parent_frame: Tkinter frame to add widgets to
        mode: 'xlsx2mysql' or 'pkl2mysql'
    
    Returns:
        dict: Dictionary containing widget references
    """
    # Clear existing widgets
    for widget in parent_frame.winfo_children():
        widget.grid_forget()
    
    widgets = {}
    
    # File path selection
    tk.Label(parent_frame, text="파일 경로:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
    widgets['entry_file_path'] = tk.Entry(parent_frame, width=30)
    widgets['entry_file_path'].grid(row=0, column=1, sticky="w", padx=5, pady=5)
    
    widgets['btn_browse'] = tk.Button(parent_frame, text="Browse", 
                                      command=lambda: browse_file(widgets, mode))
    widgets['btn_browse'].grid(row=0, column=2, padx=5, pady=5)
    
    # Import scope selection
    tk.Label(parent_frame, text="Import 범위:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
    
    widgets['var_import_scope'] = tk.StringVar(value="all")
    
    frame_scope = tk.Frame(parent_frame)
    frame_scope.grid(row=1, column=1, sticky="w", padx=5, pady=5, columnspan=2)
    
    tk.Radiobutton(frame_scope, text="특정 테이블만", variable=widgets['var_import_scope'], 
                   value="single", command=lambda: toggle_scope_widgets(widgets)).pack(side="left", padx=5)
    tk.Radiobutton(frame_scope, text="전체 (모든 키/시트)", variable=widgets['var_import_scope'], 
                   value="all", command=lambda: toggle_scope_widgets(widgets)).pack(side="left", padx=5)
    
    # Source name (Dictionary key or Sheet name) - conditional
    widgets['lbl_source_name'] = tk.Label(parent_frame, text="소스 지정:")
    widgets['entry_source_name'] = tk.Entry(parent_frame, width=30)
    
    # Help text for source
    help_text = "(Dictionary 키 또는 시트명)" if mode == "pkl2mysql" else "(시트명, 비워두면 첫 시트)"
    widgets['lbl_source_help'] = tk.Label(parent_frame, text=help_text, fg="gray", font=("", 8))
    
    # Target table name - conditional
    widgets['lbl_target_table'] = tk.Label(parent_frame, text="대상 테이블명:")
    widgets['var_target_table'] = tk.StringVar()
    widgets['entry_target_table'] = tk.Entry(parent_frame, width=30, textvariable=widgets['var_target_table'])
    
    # Import mode selection
    tk.Label(parent_frame, text="Import 모드:").grid(row=5, column=0, sticky="e", padx=5, pady=5)
    
    widgets['var_import_mode'] = tk.StringVar(value="replace")
    
    frame_mode = tk.Frame(parent_frame)
    frame_mode.grid(row=5, column=1, sticky="w", padx=5, pady=5, columnspan=2)
    
    tk.Radiobutton(frame_mode, text="Replace (대체)", variable=widgets['var_import_mode'], 
                   value="replace").pack(side="left", padx=5)
    tk.Radiobutton(frame_mode, text="Append (추가)", variable=widgets['var_import_mode'], 
                   value="append").pack(side="left", padx=5)

    # Collation selection
    tk.Label(parent_frame, text="Collation:").grid(row=6, column=0, sticky="e", padx=5, pady=5)
    widgets['var_collation'] = tk.StringVar(value="server_default")
    collations = [
        "server_default",
        "utf8mb4_uca1400_ai_ci",
        "utf8mb4_0900_ai_ci",
        "utf8mb4_unicode_ci",
        "utf8mb4_general_ci",
        "utf8mb4_bin",
    ]
    widgets['cmb_collation'] = ttk.Combobox(parent_frame, textvariable=widgets['var_collation'],
                                            values=collations, state="readonly", width=27)
    widgets['cmb_collation'].grid(row=6, column=1, sticky="w", padx=5, pady=5)
    widgets['lbl_collation_hint'] = tk.Label(parent_frame, text="(없음=DB 기본값)", fg="gray", font=("", 8))
    widgets['lbl_collation_hint'].grid(row=6, column=2, sticky="w", padx=5, pady=5)

    # Stop on collation mismatch
    widgets['var_stop_on_mismatch'] = tk.BooleanVar(value=True)
    widgets['chk_stop_on_mismatch'] = tk.Checkbutton(
        parent_frame,
        text="콜레이션 불일치 시 중단",
        variable=widgets['var_stop_on_mismatch']
    )
    widgets['chk_stop_on_mismatch'].grid(row=7, column=1, sticky="w", padx=5, pady=5)

    # Collation status (single table only)
    widgets['lbl_table_collation_title'] = tk.Label(parent_frame, text="테이블 콜레이션:")
    widgets['lbl_table_collation_value'] = tk.Label(parent_frame, text="-", fg="gray")
    widgets['lbl_collation_compare_title'] = tk.Label(parent_frame, text="비교 결과:")
    widgets['lbl_collation_compare_value'] = tk.Label(parent_frame, text="-", fg="gray")
    
    # Initial state
    toggle_scope_widgets(widgets)
    
    return widgets


def toggle_scope_widgets(widgets):
    """Show/hide source and target widgets based on import scope."""
    import_scope = widgets['var_import_scope'].get()
    
    if import_scope == "single":
        # Show source and target widgets
        widgets['lbl_source_name'].grid(row=2, column=0, sticky="e", padx=5, pady=5)
        widgets['entry_source_name'].grid(row=2, column=1, sticky="w", padx=5, pady=5)
        widgets['lbl_source_help'].grid(row=2, column=2, sticky="w", padx=5, pady=5)
        
        widgets['lbl_target_table'].grid(row=3, column=0, sticky="e", padx=5, pady=5)
        widgets['entry_target_table'].grid(row=3, column=1, sticky="w", padx=5, pady=5)

        widgets['lbl_table_collation_title'].grid(row=8, column=0, sticky="e", padx=5, pady=5)
        widgets['lbl_table_collation_value'].grid(row=8, column=1, sticky="w", padx=5, pady=5, columnspan=2)
        widgets['lbl_collation_compare_title'].grid(row=9, column=0, sticky="e", padx=5, pady=5)
        widgets['lbl_collation_compare_value'].grid(row=9, column=1, sticky="w", padx=5, pady=5, columnspan=2)
    else:
        # Hide source and target widgets
        widgets['lbl_source_name'].grid_forget()
        widgets['entry_source_name'].grid_forget()
        widgets['lbl_source_help'].grid_forget()
        
        widgets['lbl_target_table'].grid_forget()
        widgets['entry_target_table'].grid_forget()

        widgets['lbl_table_collation_title'].grid_forget()
        widgets['lbl_table_collation_value'].grid_forget()
        widgets['lbl_collation_compare_title'].grid_forget()
        widgets['lbl_collation_compare_value'].grid_forget()
        
        # Clear the entries
        widgets['entry_source_name'].delete(0, tk.END)
        widgets['var_target_table'].set("")


def browse_file(widgets, mode):
    """Browse for file based on mode."""
    filetypes = []
    if mode == "xlsx2mysql":
        filetypes = [("Excel files", "*.xlsx *.xls")]
    elif mode == "pkl2mysql":
        filetypes = [("Pickle files", "*.pkl")]
    
    filepath = filedialog.askopenfilename(filetypes=filetypes)
    if filepath:
        widgets['entry_file_path'].delete(0, tk.END)
        widgets['entry_file_path'].insert(0, filepath)


def get_import_params(widgets):
    """
    Extracts import parameters from widgets.
    
    Args:
        widgets: Dictionary of widget references from create_import_widgets
    
    Returns:
        dict: {'file_path': str, 'import_scope': str, 'source_name': str or None, 
               'target_table': str or None, 'if_exists': str}
    """
    file_path = widgets['entry_file_path'].get().strip()
    import_scope = widgets['var_import_scope'].get()
    source_name = widgets['entry_source_name'].get().strip()
    target_table = widgets['entry_target_table'].get().strip()
    if_exists = widgets['var_import_mode'].get()
    collation = widgets['var_collation'].get()
    stop_on_mismatch = widgets['var_stop_on_mismatch'].get()
    
    if not file_path:
        return None  # Validation failed
    
    return {
        'file_path': file_path,
        'import_scope': import_scope,
        'source_name': source_name if source_name else None,
        'target_table': target_table if target_table else None,
        'if_exists': if_exists,
        'collation': collation if collation else "server_default",
        'stop_on_mismatch': bool(stop_on_mismatch)
    }

# --- Main Tab Entry Point ---

def create_mysql_tab(notebook, app_instance):
    """
    Creates the MySQL tab content.
    
    Args:
        notebook: The parent ttk.Notebook
        app_instance: Instance of the main application class (for callbacks/references)
    
    Returns:
        tk.Frame: The frame containing the MySQL tab content
    """
    tab = tk.Frame(notebook)
    
    # --- Main Layout (Left: Settings/Log, Right: Query) ---
    paned_window = tk.PanedWindow(tab, orient=tk.HORIZONTAL)
    paned_window.pack(fill="both", expand=True, padx=10, pady=10)
    
    # Left Panel (Settings & Log)
    left_panel = tk.Frame(paned_window)
    paned_window.add(left_panel, width=500)
    
    # Right Panel (Query Input) - Initially hidden
    right_panel = tk.Frame(paned_window)
    # Store references in app_instance for toggling
    app_instance.mysql_paned_window = paned_window
    app_instance.mysql_right_panel = right_panel
    app_instance.is_query_panel_visible = False

    # --- DB Connection Section (Left Panel) ---
    lb_db_frame = tk.LabelFrame(left_panel, text="DB Connection Settings", padx=10, pady=10)
    lb_db_frame.pack(fill="x", padx=10, pady=5)

    tk.Label(lb_db_frame, text="DB Name:").grid(row=0, column=0, sticky="e")
    app_instance.entry_db_name = tk.Entry(lb_db_frame)
    import os
    app_instance.entry_db_name.insert(0, os.getenv("MYSQL_DB", ""))
    app_instance.entry_db_name.grid(row=0, column=1, sticky="w", padx=5)

    app_instance.var_prod = tk.BooleanVar(value=True)
    chk_prod = tk.Checkbutton(lb_db_frame, text="Use Prod DB", variable=app_instance.var_prod, command=app_instance.update_db_info)
    chk_prod.grid(row=1, column=0, columnspan=2, sticky="w")
    
    app_instance.lbl_db_info = tk.Label(lb_db_frame, text="", fg="gray")
    app_instance.lbl_db_info.grid(row=2, column=0, columnspan=2, sticky="w")
    app_instance.update_db_info()

    # --- Mode Selection Section (Left Panel) ---
    lb_mode_frame = tk.LabelFrame(left_panel, text="Select Mode", padx=10, pady=10)
    lb_mode_frame.pack(fill="x", padx=10, pady=5)

    app_instance.var_mode = tk.StringVar(value="mysql2xlsx")
    
    modes = [
        ("MySQL -> Excel", "mysql2xlsx"),
        ("MySQL -> Pickle", "mysql2pkl"),
        ("Excel -> MySQL", "xlsx2mysql"),
        ("Pickle -> MySQL", "pkl2mysql"),
    ]

    for text, value in modes:
        tk.Radiobutton(lb_mode_frame, text=text, variable=app_instance.var_mode, value=value, command=app_instance.update_ui).pack(anchor="w")

    # --- Dynamic Input Section (Left Panel) ---
    app_instance.lb_input_frame = tk.LabelFrame(left_panel, text="Settings", padx=10, pady=10)
    app_instance.lb_input_frame.pack(fill="x", padx=10, pady=5)

    # --- Log Output Section (Left Panel) ---
    lb_log_frame = tk.LabelFrame(left_panel, text="Log Output", padx=10, pady=10)
    lb_log_frame.pack(fill="both", expand=True, padx=10, pady=5)
    
    import tkinter.scrolledtext as scrolledtext
    import sys
    
    log_text = scrolledtext.ScrolledText(lb_log_frame, height=15, state='disabled', wrap='word')
    log_text.pack(fill="both", expand=True)
    
    # Redirect stdout to a custom redirector that writes to this widget
    # Note: We need a way to switch stdout target if tabs switch, or just use one global log?
    # For now, let's assume global stdout redirection is handled by main app or we just use one log per tab
    # But print() goes to sys.stdout. If we have multiple tabs, valid question.
    # User requirement: "Independent workspaces".
    # Let's attach log_text to app_instance to potentially switch redirection.

    app_instance.log_text = log_text
    
    # --- Action Button (Left Panel) ---
    tk.Button(left_panel, text="RUN", command=app_instance.run_process, height=2, bg="#dddddd").pack(fill="x", padx=10, pady=10)

    # --- Query Input Section (Right Panel) ---
    lb_query_frame = tk.LabelFrame(right_panel, text="SQL Query Input (For Query Mode)", padx=10, pady=10)
    lb_query_frame.pack(fill="both", expand=True, padx=10, pady=5)
    
    app_instance.txt_query = scrolledtext.ScrolledText(lb_query_frame, font=("Consolas", 10))
    app_instance.txt_query.pack(fill="both", expand=True)
    
    tk.Label(lb_query_frame, text="* Export 범위에서 '사용자 정의 쿼리' 선택 시 사용됩니다.", fg="gray").pack(anchor="w")
    
    return tab
