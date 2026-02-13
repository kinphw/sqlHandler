import tkinter as tk
from tkinter import filedialog

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
    widgets['entry_target_table'] = tk.Entry(parent_frame, width=30)
    
    # Import mode selection
    tk.Label(parent_frame, text="Import 모드:").grid(row=5, column=0, sticky="e", padx=5, pady=5)
    
    widgets['var_import_mode'] = tk.StringVar(value="replace")
    
    frame_mode = tk.Frame(parent_frame)
    frame_mode.grid(row=5, column=1, sticky="w", padx=5, pady=5, columnspan=2)
    
    tk.Radiobutton(frame_mode, text="Replace (대체)", variable=widgets['var_import_mode'], 
                   value="replace").pack(side="left", padx=5)
    tk.Radiobutton(frame_mode, text="Append (추가)", variable=widgets['var_import_mode'], 
                   value="append").pack(side="left", padx=5)
    
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
    else:
        # Hide source and target widgets
        widgets['lbl_source_name'].grid_forget()
        widgets['entry_source_name'].grid_forget()
        widgets['lbl_source_help'].grid_forget()
        
        widgets['lbl_target_table'].grid_forget()
        widgets['entry_target_table'].grid_forget()
        
        # Clear the entries
        widgets['entry_source_name'].delete(0, tk.END)
        widgets['entry_target_table'].delete(0, tk.END)


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
    
    if not file_path:
        return None  # Validation failed
    
    return {
        'file_path': file_path,
        'import_scope': import_scope,
        'source_name': source_name if source_name else None,
        'target_table': target_table if target_table else None,
        'if_exists': if_exists
    }
