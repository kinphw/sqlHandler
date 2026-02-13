import tkinter as tk

def create_export_widgets(parent_frame, mode):
    """
    Creates GUI widgets for export operations (MySQL -> Excel/Pickle).
    
    Args:
        parent_frame: Tkinter frame to add widgets to
        mode: 'mysql2xlsx' or 'mysql2pkl'
    
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
                   value="table", command=lambda: toggle_table_entry()).pack(side="left", padx=5)
    tk.Radiobutton(frame_scope, text="전체 데이터베이스", variable=widgets['var_export_scope'], 
                   value="database", command=lambda: toggle_table_entry()).pack(side="left", padx=5)
    
    # Table name input
    tk.Label(parent_frame, text="테이블명:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
    widgets['entry_table_name'] = tk.Entry(parent_frame, width=30)
    widgets['entry_table_name'].grid(row=1, column=1, sticky="w", padx=5, pady=5)
    
    def toggle_table_entry():
        if widgets['var_export_scope'].get() == "table":
            widgets['entry_table_name'].config(state="normal")
        else:
            widgets['entry_table_name'].delete(0, tk.END)
            widgets['entry_table_name'].config(state="disabled")
    
    # Initial state
    toggle_table_entry()
    
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
        return {'table_name': table_name}
    else:
        # Full database export
        return {'table_name': None}
