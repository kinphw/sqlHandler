
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import os
import sys

# Import SQLite modules
from sqlite.tosqlite.xlsx2sqlite import import_from_xlsx
from sqlite.tosqlite.pkl2sqlite import import_from_pkl
from sqlite.fromsqlite.sqlite2xlsx import export_to_xlsx
from sqlite.fromsqlite.sqlite2pkl import export_to_pkl
from sqlite.utils.convert_db_to_base64 import convert_db_to_js

def create_sqlite_tab(notebook):
    """
    Creates the SQLite tab content.
    Returns: tk.Frame
    """
    tab = tk.Frame(notebook)
    
    # Instance storage for this tab's logic
    logic = SQLiteTabLogic(tab)
    
    # --- Layout ---
    # Left: Settings, Right: Log/Query? 
    # For SQLite, maybe we just use a similar layout.
    
    paned_window = tk.PanedWindow(tab, orient=tk.HORIZONTAL)
    paned_window.pack(fill="both", expand=True, padx=10, pady=10)
    
    left_panel = tk.Frame(paned_window)
    paned_window.add(left_panel, width=500)
    
    right_panel = tk.Frame(paned_window)
    paned_window.add(right_panel)
    
    # --- SQLite DB Selection (Left) ---
    lb_db_frame = tk.LabelFrame(left_panel, text="SQLite DB File", padx=10, pady=10)
    lb_db_frame.pack(fill="x", padx=10, pady=5)
    
    logic.entry_db_path = tk.Entry(lb_db_frame)
    logic.entry_db_path.pack(side="left", fill="x", expand=True, padx=5)
    
    tk.Button(lb_db_frame, text="Browse", command=logic.browse_db).pack(side="left", padx=5)
    tk.Button(lb_db_frame, text="New", command=logic.create_new_db).pack(side="left", padx=5)
    
    # --- Mode Selection (Left) ---
    lb_mode_frame = tk.LabelFrame(left_panel, text="Operation Mode", padx=10, pady=10)
    lb_mode_frame.pack(fill="x", padx=10, pady=5)
    
    logic.var_mode = tk.StringVar(value="xlsx2sqlite")
    
    modes = [
        ("Excel -> SQLite (Import)", "xlsx2sqlite"),
        ("Pickle -> SQLite (Import)", "pkl2sqlite"),
        ("SQLite -> Excel (Export)", "sqlite2xlsx"),
        ("SQLite -> Pickle (Export)", "sqlite2pkl"),
        ("Util: DB -> Base64 JS", "db2js"),
    ]
    
    for text, value in modes:
        tk.Radiobutton(lb_mode_frame, text=text, variable=logic.var_mode, 
                       value=value, command=logic.update_ui).pack(anchor="w")
                       
    # --- Dynamic Settings (Left) ---
    logic.lb_settings_frame = tk.LabelFrame(left_panel, text="Settings", padx=10, pady=10)
    logic.lb_settings_frame.pack(fill="x", padx=10, pady=5)
    
    # --- Action Button (Left) ---
    tk.Button(left_panel, text="RUN OPERATION", command=logic.run_process, 
              height=2, bg="#dddddd").pack(fill="x", padx=10, pady=10)
              
    # --- Log/Query (Right) ---
    lb_log_frame = tk.LabelFrame(right_panel, text="Log / Query Input", padx=10, pady=10)
    lb_log_frame.pack(fill="both", expand=True, padx=10, pady=5)
    
    logic.txt_log = scrolledtext.ScrolledText(lb_log_frame, height=20)
    logic.txt_log.pack(fill="both", expand=True)
    
    # Initial Update
    logic.update_ui()
    
    return tab

class SQLiteTabLogic:
    def __init__(self, root_frame):
        self.root = root_frame
        self.widgets = {} # config widgets
        
    def log(self, message):
        self.txt_log.insert(tk.END, message + "\n")
        self.txt_log.see(tk.END)
        
    def browse_db(self):
        f = filedialog.askopenfilename(filetypes=[("SQLite DB", "*.db"), ("All Files", "*.*")])
        if f:
            self.entry_db_path.delete(0, tk.END)
            self.entry_db_path.insert(0, f)
            
    def create_new_db(self):
        f = filedialog.asksaveasfilename(defaultextension=".db", filetypes=[("SQLite DB", "*.db")])
        if f:
            self.entry_db_path.delete(0, tk.END)
            self.entry_db_path.insert(0, f)
            
    def update_ui(self):
        # Clear settings frame
        for w in self.lb_settings_frame.winfo_children():
            w.destroy()
            
        mode = self.var_mode.get()
        self.widgets = {}
        
        if mode in ["xlsx2sqlite", "pkl2sqlite"]:
            # Import UI
            tk.Label(self.lb_settings_frame, text="Source File:").pack(anchor="w")
            
            f_frame = tk.Frame(self.lb_settings_frame)
            f_frame.pack(fill="x")
            self.widgets['entry_file'] = tk.Entry(f_frame)
            self.widgets['entry_file'].pack(side="left", fill="x", expand=True)
            tk.Button(f_frame, text="Browse", 
                      command=lambda: self.browse_file_import(mode)).pack(side="left")
            
            tk.Label(self.lb_settings_frame, text="Table Name (Optional):").pack(anchor="w")
            self.widgets['entry_table'] = tk.Entry(self.lb_settings_frame)
            self.widgets['entry_table'].pack(fill="x")
            
            self.widgets['var_if_exists'] = tk.StringVar(value="replace")
            tk.Radiobutton(self.lb_settings_frame, text="Replace", 
                           variable=self.widgets['var_if_exists'], value="replace").pack(anchor="w")
            tk.Radiobutton(self.lb_settings_frame, text="Append", 
                           variable=self.widgets['var_if_exists'], value="append").pack(anchor="w")

        elif mode in ["sqlite2xlsx", "sqlite2pkl"]:
            # Export UI
            tk.Label(self.lb_settings_frame, text="Export Scope:").pack(anchor="w")
            self.widgets['var_scope'] = tk.StringVar(value="table")
            
            tk.Radiobutton(self.lb_settings_frame, text="Specific Table", 
                           variable=self.widgets['var_scope'], value="table").pack(anchor="w")
            tk.Radiobutton(self.lb_settings_frame, text="Whole Database", 
                           variable=self.widgets['var_scope'], value="database").pack(anchor="w")
            tk.Radiobutton(self.lb_settings_frame, text="Custom Query", 
                           variable=self.widgets['var_scope'], value="query").pack(anchor="w")
                           
            tk.Label(self.lb_settings_frame, text="Table Name:").pack(anchor="w")
            self.widgets['entry_table'] = tk.Entry(self.lb_settings_frame)
            self.widgets['entry_table'].pack(fill="x")
            
            tk.Label(self.lb_settings_frame, text="Query (Write in Right Panel):").pack(anchor="w")
            
        elif mode == "db2js":
            tk.Label(self.lb_settings_frame, text="Convert DB to Base64 JS for Web").pack()

    def browse_file_import(self, mode):
        ft = [("Excel", "*.xlsx *.xls")] if mode == "xlsx2sqlite" else [("Pickle", "*.pkl")]
        f = filedialog.askopenfilename(filetypes=ft)
        if f:
            self.widgets['entry_file'].delete(0, tk.END)
            self.widgets['entry_file'].insert(0, f)

    def run_process(self):
        db_path = self.entry_db_path.get().strip()
        if not db_path:
            messagebox.showwarning("Warning", "Select a SQLite DB file first.")
            return

        mode = self.var_mode.get()
        
        try:
            if mode in ["xlsx2sqlite", "pkl2sqlite"]:
                src_file = self.widgets['entry_file'].get()
                if not src_file: return
                
                table = self.widgets['entry_table'].get().strip()
                if_exists = self.widgets['var_if_exists'].get()
                
                # Logic: if table name is given, scope='single', else 'all'
                scope = "single" if table else "all"
                
                self.log(f"Starting Import: {src_file} -> {db_path} ({scope})")
                
                if mode == "xlsx2sqlite":
                    import_from_xlsx(db_path, src_file, scope, target_table=table, if_exists=if_exists)
                else:
                    import_from_pkl(db_path, src_file, scope, target_table=table, if_exists=if_exists)
                    
                self.log("Import Success!")
                messagebox.showinfo("Success", "SQLite Import Completed.")

            elif mode in ["sqlite2xlsx", "sqlite2pkl"]:
                scope = self.widgets['var_scope'].get()
                table = self.widgets['entry_table'].get().strip()
                query = self.txt_log.get("1.0", tk.END).strip() if scope == "query" else None
                
                ext = ".xlsx" if mode == "sqlite2xlsx" else ".pkl"
                f = filedialog.asksaveasfilename(defaultextension=ext)
                if not f: return
                
                self.log(f"Starting Export: {scope} -> {f}")
                
                if mode == "sqlite2xlsx":
                    export_to_xlsx(db_path, scope, table, query, f)
                else:
                    export_to_pkl(db_path, scope, table, query, f)
                    
                self.log("Export Success!")
                messagebox.showinfo("Success", "SQLite Export Completed.")

            elif mode == "db2js":
                f = filedialog.asksaveasfilename(defaultextension=".js", initialfile=os.path.basename(db_path).replace('.db','.js'))
                if not f: return
                
                self.log("Converting DB to JS...")
                convert_db_to_js(db_path, f)
                self.log("Conversion Success!")
                messagebox.showinfo("Success", "DB Converted to JS.")
                
        except Exception as e:
            self.log(f"Error: {str(e)}")
            messagebox.showerror("Error", str(e))
