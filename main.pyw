import tkinter as tk
from tkinter import filedialog, messagebox, ttk, scrolledtext
import os
import sys
from dotenv import load_dotenv
import pymysql

# Load Modules
from frommysql.mysql2xlsx import export_to_xlsx
from frommysql.mysql2pkl import export_to_pkl
from frommysql.gui_widgets import create_export_widgets, get_export_params
from tomysql.xlsx2mysql import import_from_xlsx
from tomysql.pkl2mysql import import_from_pkl
from tomysql.gui_widgets import create_import_widgets, get_import_params

# Load .env
load_dotenv('.env')

class MySQLHandlerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("MySQL Data Handler")
        self.root.geometry("1000x750")
        
        # Center Window
        self.center_window()

        # --- Main Layout (Left: Settings/Log, Right: Query) ---
        self.paned_window = tk.PanedWindow(root, orient=tk.HORIZONTAL)
        self.paned_window.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Left Panel (Settings & Log)
        self.left_panel = tk.Frame(self.paned_window)
        self.paned_window.add(self.left_panel, width=500)
        
        # Right Panel (Query Input) - Initially hidden
        self.right_panel = tk.Frame(self.paned_window)
        self.is_query_panel_visible = False  # Track visibility state

        # --- DB Connection Section (Left Panel) ---
        lb_db_frame = tk.LabelFrame(self.left_panel, text="DB Connection Settings", padx=10, pady=10)
        lb_db_frame.pack(fill="x", padx=10, pady=5)

        tk.Label(lb_db_frame, text="DB Name:").grid(row=0, column=0, sticky="e")
        self.entry_db_name = tk.Entry(lb_db_frame)
        self.entry_db_name.insert(0, os.getenv("MYSQL_DB", ""))
        self.entry_db_name.grid(row=0, column=1, sticky="w", padx=5)

        self.var_prod = tk.BooleanVar(value=True)
        self.chk_prod = tk.Checkbutton(lb_db_frame, text="Use Prod DB", variable=self.var_prod, command=self.update_db_info)
        self.chk_prod.grid(row=1, column=0, columnspan=2, sticky="w")
        
        self.lbl_db_info = tk.Label(lb_db_frame, text="", fg="gray")
        self.lbl_db_info.grid(row=2, column=0, columnspan=2, sticky="w")
        self.update_db_info()

        # --- Mode Selection Section (Left Panel) ---
        lb_mode_frame = tk.LabelFrame(self.left_panel, text="Select Mode", padx=10, pady=10)
        lb_mode_frame.pack(fill="x", padx=10, pady=5)

        self.var_mode = tk.StringVar(value="mysql2xlsx")
        
        modes = [
            ("MySQL -> Excel", "mysql2xlsx"),
            ("MySQL -> Pickle", "mysql2pkl"),
            ("Excel -> MySQL", "xlsx2mysql"),
            ("Pickle -> MySQL", "pkl2mysql"),
        ]

        for text, value in modes:
            tk.Radiobutton(lb_mode_frame, text=text, variable=self.var_mode, value=value, command=self.update_ui).pack(anchor="w")

        # --- Dynamic Input Section (Left Panel) ---
        self.lb_input_frame = tk.LabelFrame(self.left_panel, text="Settings", padx=10, pady=10)
        self.lb_input_frame.pack(fill="x", padx=10, pady=5)

        # Widget references (populated by update_ui)
        self.widgets = {}
        
        # Initial UI Setup
        self.update_ui()

        # --- Log Output Section (Left Panel) ---
        lb_log_frame = tk.LabelFrame(self.left_panel, text="Log Output", padx=10, pady=10)
        lb_log_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.log_text = scrolledtext.ScrolledText(lb_log_frame, height=15, state='disabled', wrap='word')
        self.log_text.pack(fill="both", expand=True)
        
        # Redirect stdout to log widget
        sys.stdout = TextRedirector(self.log_text)
        
        # --- Action Button (Left Panel) ---
        tk.Button(self.left_panel, text="RUN", command=self.run_process, height=2, bg="#dddddd").pack(fill="x", padx=10, pady=10)

        # --- Query Input Section (Right Panel) ---
        lb_query_frame = tk.LabelFrame(self.right_panel, text="SQL Query Input (For Query Mode)", padx=10, pady=10)
        lb_query_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.txt_query = scrolledtext.ScrolledText(lb_query_frame, font=("Consolas", 10))
        self.txt_query.pack(fill="both", expand=True)
        
        tk.Label(lb_query_frame, text="* Export 범위에서 '사용자 정의 쿼리' 선택 시 사용됩니다.", fg="gray").pack(anchor="w")

    def center_window(self):
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')

    def update_db_info(self):
        prefix = "PROD_" if self.var_prod.get() else ""
        host = os.getenv(f"{prefix}MYSQL_HOST")
        port = os.getenv(f"{prefix}MYSQL_PORT")
        user = os.getenv(f"{prefix}MYSQL_USER")
        self.lbl_db_info.config(text=f"Connecting to: {user}@{host}:{port}")

    def update_ui(self):
        """Update UI based on selected mode using modular GUI widgets."""
        mode = self.var_mode.get()
        
        # Hide query panel by default
        self.toggle_query_panel(False)
        
        if mode in ["mysql2xlsx", "mysql2pkl"]:
            # Export Mode: Use frommysql GUI widgets
            # Pass callback to toggle query panel
            self.widgets = create_export_widgets(self.lb_input_frame, mode, 
                                               on_query_mode_change=self.toggle_query_panel)
        elif mode in ["xlsx2mysql", "pkl2mysql"]:
            # Import Mode: Use tomysql GUI widgets
            self.widgets = create_import_widgets(self.lb_input_frame, mode)

    def toggle_query_panel(self, show):
        """Show or hide the right-side query panel."""
        if show:
            if not self.is_query_panel_visible:
                self.paned_window.add(self.right_panel)
                self.is_query_panel_visible = True
        else:
            if self.is_query_panel_visible:
                self.paned_window.forget(self.right_panel)
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
        db_url, db_config = self.get_db_url_and_config()
        if not db_url: return

        mode = self.var_mode.get()
        
        try:
            if mode in ["mysql2xlsx", "mysql2pkl"]:
                # Export operations
                params = get_export_params(self.widgets)
                if params is None:
                    messagebox.showwarning("Warning", "특정 테이블 선택 시 테이블명을 입력해주세요.")
                    return
                
                export_scope = params.get('scope', 'table')  # Default to table for backward compatibility
                table_name = params['table_name']
                query = None

                # Handle Query Mode
                if export_scope == 'query':
                    query = self.txt_query.get("1.0", tk.END).strip()
                    if not query:
                        messagebox.showwarning("Warning", "쿼리를 입력해주세요.")
                        return

                # Determine file extension and filter
                if mode == "mysql2xlsx":
                    ext = ".xlsx"
                    filetypes = [("Excel files", "*.xlsx")]
                else:
                    ext = ".pkl"
                    filetypes = [("Pickle files", "*.pkl")]
                
                # Default filename
                if export_scope == 'query':
                    default_name = f"query_result{ext}"
                elif table_name:
                    default_name = f"{table_name}{ext}"
                else:
                    db_name = self.entry_db_name.get()
                    default_name = f"{db_name}_full{ext}"
                
                # Ask for save path
                save_path = filedialog.asksaveasfilename(
                    defaultextension=ext, 
                    filetypes=filetypes, 
                    initialfile=default_name
                )
                if not save_path: return
                
                # Execute export
                if mode == "mysql2xlsx":
                    export_to_xlsx(db_url, export_scope, table_name, query, save_path)
                else:
                    export_to_pkl(db_url, export_scope, table_name, query, save_path)
                
                if export_scope == 'query':
                    scope_text = "사용자 정의 쿼리 결과"
                elif table_name:
                    scope_text = f"테이블 '{table_name}'"
                else:
                    scope_text = "전체 데이터베이스"
                    
                messagebox.showinfo("Success", f"{scope_text}를 {save_path}로 추출했습니다.")

            elif mode in ["xlsx2mysql", "pkl2mysql"]:
                # Import operations
                params = get_import_params(self.widgets)
                if params is None:
                    messagebox.showwarning("Warning", "파일을 선택해주세요.")
                    return
                
                file_path = params['file_path']
                import_scope = params['import_scope']
                source_name = params['source_name']
                target_table = params['target_table']
                if_exists = params['if_exists']
                
                # Validation for single mode
                if import_scope == "single" and not target_table:
                    messagebox.showwarning("Warning", "특정 테이블 모드에서는 대상 테이블명을 입력해주세요.")
                    return
                
                # Execute import
                if mode == "xlsx2mysql":
                    import_from_xlsx(db_url, file_path, import_scope, source_name, target_table, if_exists)
                else:
                    import_from_pkl(db_config, file_path, import_scope, source_name, target_table, if_exists)
                
                scope_text = f"테이블 '{target_table}'" if import_scope == "single" else "전체 데이터"
                mode_text = "대체" if if_exists == "replace" else "추가"
                messagebox.showinfo("Success", f"{scope_text} Import가 완료되었습니다 ({mode_text} 모드).")

        except Exception as e:
            messagebox.showerror("Error", f"오류가 발생했습니다:\n{str(e)}")


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
    app = MySQLHandlerApp(root)
    root.mainloop()
