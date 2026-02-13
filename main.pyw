import tkinter as tk
from tkinter import filedialog, messagebox, ttk, scrolledtext
import os
import sys
from dotenv import load_dotenv
import pymysql

# Load Modules
from mysql.frommysql.mysql2xlsx import export_to_xlsx
from mysql.frommysql.mysql2pkl import export_to_pkl
from mysql.tomysql.xlsx2mysql import import_from_xlsx as mysql_import_xlsx
from mysql.tomysql.pkl2mysql import import_from_pkl as mysql_import_pkl
# Updated import paths for MySQL widgets
from mysql.gui_widgets import create_export_widgets, get_export_params, create_import_widgets, get_import_params, create_mysql_tab

# Load SQLite Modules
from sqlite.gui_widgets import create_sqlite_tab

# Load .env
load_dotenv('.env')

class DataHandlerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("SQL Data Handler (MySQL & SQLite)")
        self.root.geometry("1000x800")
        
        # Center Window
        self.center_window()

        # --- Main Layout: Notebook (Tabs) ---
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)
        
        # --- Tab 1: MySQL ---
        self.tab_mysql = create_mysql_tab(self.notebook, self)
        self.notebook.add(self.tab_mysql, text="MySQL Handler")
        
        # --- Tab 2: SQLite ---
        self.tab_sqlite = create_sqlite_tab(self.notebook)
        self.notebook.add(self.tab_sqlite, text="SQLite Handler")
        
        # --- Redirect stdout to MySQL log by default (or handle globally) ---
        # For simple integration, we redirect stdout to the MySQL log widget when that tab is active
        # But `sys.stdout` is global. A better way is a custom redirector that writes to the currently active tab's log.
        # For now, let's keep it simple: Write to MySQL log if available.
        # SQLite tab has its own log logic in its class.
        if hasattr(self, 'log_text'):
             sys.stdout = TextRedirector(self.log_text)

    def center_window(self):
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')

    # --- MySQL Logic Methods (Called by mysql/gui_widgets.py) ---

    def update_db_info(self):
        prefix = "PROD_" if self.var_prod.get() else ""
        host = os.getenv(f"{prefix}MYSQL_HOST")
        port = os.getenv(f"{prefix}MYSQL_PORT")
        user = os.getenv(f"{prefix}MYSQL_USER")
        self.lbl_db_info.config(text=f"Connecting to: {user}@{host}:{port}")

    def update_ui(self):
        """Update MySQL UI based on selected mode."""
        mode = self.var_mode.get()
        
        # Hide query panel by default
        self.toggle_query_panel(False)
        
        if mode in ["mysql2xlsx", "mysql2pkl"]:
            # Export Mode
            self.widgets = create_export_widgets(self.lb_input_frame, mode, 
                                               on_query_mode_change=self.toggle_query_panel)
        elif mode in ["xlsx2mysql", "pkl2mysql"]:
            # Import Mode
            self.widgets = create_import_widgets(self.lb_input_frame, mode)

    def toggle_query_panel(self, show):
        """Show or hide the right-side query panel in MySQL tab."""
        if show:
            if not self.is_query_panel_visible:
                self.mysql_paned_window.add(self.mysql_right_panel)
                self.is_query_panel_visible = True
        else:
            if self.is_query_panel_visible:
                self.mysql_paned_window.forget(self.mysql_right_panel)
                self.is_query_panel_visible = False

    def get_db_url_and_config(self):
        db_name = self.entry_db_name.get()
        if not db_name:
            messagebox.showerror("Error", "Database name is required.")
            return None, None
            
        prefix = "PROD_" if self.var_prod.get() else ""
        
        config = {
            'user': os.getenv(f"{prefix}MYSQL_USER"),
            'password': os.getenv(f"{prefix}MYSQL_PASSWORD"),
            'host': os.getenv(f"{prefix}MYSQL_HOST"),
            'port': os.getenv(f"{prefix}MYSQL_PORT"),
            'database': db_name
        }
        
        # Validation
        if not all(config.values()):
             messagebox.showerror("Configuration Error", "Missing .env configuration for selected environment.")
             return None, None

        db_url = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}?charset=utf8mb4"
        return db_url, config

    def run_process(self):
        """Execute MySQL Process"""
        db_url, db_config = self.get_db_url_and_config()
        if not db_url: return

        mode = self.var_mode.get()
        
        try:
            if mode in ["mysql2xlsx", "mysql2pkl"]:
                # Export operations
                params = get_export_params(self.widgets)
                if params is None:
                    messagebox.showwarning("Warning", "Check input fields.")
                    return
                
                export_scope = params.get('scope', 'table')
                table_name = params['table_name']
                query = None

                if export_scope == 'query':
                    query = self.txt_query.get("1.0", tk.END).strip()
                    if not query:
                        messagebox.showwarning("Warning", "Enter a query.")
                        return

                if mode == "mysql2xlsx":
                    ext = ".xlsx"
                    filetypes = [("Excel files", "*.xlsx")]
                else:
                    ext = ".pkl"
                    filetypes = [("Pickle files", "*.pkl")]
                
                default_name = "output" + ext
                if export_scope == 'query': default_name = "query_result" + ext
                elif table_name: default_name = table_name + ext
                else: default_name = self.entry_db_name.get() + "_full" + ext
                
                save_path = filedialog.asksaveasfilename(defaultextension=ext, filetypes=filetypes, initialfile=default_name)
                if not save_path: return
                
                if mode == "mysql2xlsx":
                    export_to_xlsx(db_url, export_scope, table_name, query, save_path)
                else:
                    export_to_pkl(db_url, export_scope, table_name, query, save_path)
                    
                messagebox.showinfo("Success", f"Export to {save_path} successful.")

            elif mode in ["xlsx2mysql", "pkl2mysql"]:
                # Import operations
                params = get_import_params(self.widgets)
                if params is None:
                    messagebox.showwarning("Warning", "Select a file.")
                    return
                
                # Validation for single mode
                if params['import_scope'] == "single" and not params['target_table']:
                    messagebox.showwarning("Warning", "Target table name required.")
                    return
                
                if mode == "xlsx2mysql":
                    mysql_import_xlsx(db_url, params['file_path'], params['import_scope'], params['source_name'], params['target_table'], params['if_exists'])
                else:
                    mysql_import_pkl(db_config, params['file_path'], params['import_scope'], params['source_name'], params['target_table'], params['if_exists'])
                
                messagebox.showinfo("Success", "Import successful.")

        except Exception as e:
            messagebox.showerror("Error", f"An error occurred:\n{str(e)}")


class TextRedirector:
    """Redirects stdout to a tkinter Text widget."""
    def __init__(self, widget):
        self.widget = widget

    def write(self, text):
        self.widget.configure(state='normal')
        self.widget.insert(tk.END, text)
        self.widget.see(tk.END)
        self.widget.configure(state='disabled')
        self.widget.update_idletasks()

    def flush(self):
        pass


if __name__ == "__main__":
    root = tk.Tk()
    app = DataHandlerApp(root)
    root.mainloop()
