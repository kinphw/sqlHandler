import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
from dotenv import load_dotenv
import pymysql

# Load Modules
from frommysql.mysql2xlsx import export_to_xlsx
from frommysql.mysql2pkl import export_to_pkl
from tomysql.xlsx2mysql import import_from_xlsx
from tomysql.pkl2mysql import import_from_pkl

# Load .env
load_dotenv('.env')

class MySQLHandlerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("MySQL Data Handler")
        self.root.geometry("500x450")
        
        # Center Window
        self.center_window()

        # --- DB Connection Section ---
        lb_db_frame = tk.LabelFrame(root, text="DB Connection Settings", padx=10, pady=10)
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

        # --- Mode Selection Section ---
        lb_mode_frame = tk.LabelFrame(root, text="Select Mode", padx=10, pady=10)
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

        # --- Dynamic Input Section ---
        self.lb_input_frame = tk.LabelFrame(root, text="Settings", padx=10, pady=10)
        self.lb_input_frame.pack(fill="x", padx=10, pady=5)

        # Dynamic Widgets
        self.lbl_target = tk.Label(self.lb_input_frame, text="")
        self.entry_target = tk.Entry(self.lb_input_frame, width=30)
        self.btn_browse = tk.Button(self.lb_input_frame, text="Browse", command=self.browse_file)
        
        # Initial UI Setup
        self.update_ui()

        # --- Action Button ---
        tk.Button(root, text="RUN", command=self.run_process, height=2, bg="#dddddd").pack(fill="x", padx=10, pady=10)

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
        # Clear frame
        for widget in self.lb_input_frame.winfo_children():
            widget.grid_forget()

        mode = self.var_mode.get()
        
        if mode in ["mysql2xlsx", "mysql2pkl"]:
            # Export Mode: Need Table Name
            self.lbl_target.config(text="Table Name:")
            self.lbl_target.grid(row=0, column=0, sticky="e", padx=5)
            self.entry_target.grid(row=0, column=1, sticky="w", padx=5)
            self.entry_target.delete(0, tk.END) # Clear
            # Browse button not needed for table name
            
        elif mode in ["xlsx2mysql", "pkl2mysql"]:
            # Import Mode: Need File Path & Optional Table Name (for xlsx mostly, or forced override)
            self.lbl_target.config(text="File Path:")
            self.lbl_target.grid(row=0, column=0, sticky="e", padx=5)
            self.entry_target.grid(row=0, column=1, sticky="w", padx=5)
            self.btn_browse.grid(row=0, column=2, padx=5)
            
            # Optional Table Name for Import
            tk.Label(self.lb_input_frame, text="Target Table (Opt):").grid(row=1, column=0, sticky="e", padx=5, pady=5)
            self.entry_target_table = tk.Entry(self.lb_input_frame, width=20)
            self.entry_target_table.grid(row=1, column=1, sticky="w", padx=5, pady=5)


    def browse_file(self):
        mode = self.var_mode.get()
        filetypes = []
        if mode == "xlsx2mysql":
            filetypes = [("Excel files", "*.xlsx *.xls")]
        elif mode == "pkl2mysql":
            filetypes = [("Pickle files", "*.pkl")]
            
        filepath = filedialog.askopenfilename(filetypes=filetypes)
        if filepath:
            self.entry_target.delete(0, tk.END)
            self.entry_target.insert(0, filepath)

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
            if mode == "mysql2xlsx":
                table_name = self.entry_target.get()
                if not table_name:
                    messagebox.showwarning("Warning", "Please enter a table name.")
                    return
                
                # Ask for save path
                save_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")], initialfile=f"{table_name}.xlsx")
                if not save_path: return
                
                export_to_xlsx(db_url, table_name, save_path)
                messagebox.showinfo("Success", f"Exported {table_name} to {save_path}")

            elif mode == "mysql2pkl":
                table_name = self.entry_target.get()
                if not table_name:
                    messagebox.showwarning("Warning", "Please enter a table name.")
                    return

                # Ask for save path
                save_path = filedialog.asksaveasfilename(defaultextension=".pkl", filetypes=[("Pickle files", "*.pkl")], initialfile=f"{table_name}.pkl")
                if not save_path: return

                export_to_pkl(db_url, table_name, save_path)
                messagebox.showinfo("Success", f"Exported {table_name} to {save_path}")

            elif mode == "xlsx2mysql":
                file_path = self.entry_target.get()
                if not file_path:
                    messagebox.showwarning("Warning", "Please select an Excel file.")
                    return
                
                target_table = self.entry_target_table.get()
                target_table = target_table if target_table else None
                
                import_from_xlsx(db_url, file_path, target_table)
                messagebox.showinfo("Success", "Import completed.")

            elif mode == "pkl2mysql":
                file_path = self.entry_target.get()
                if not file_path:
                    messagebox.showwarning("Warning", "Please select a Pickle file.")
                    return
                
                target_table = self.entry_target_table.get()
                if not target_table:
                    # Unlike xlsx, pkl doesn't have sheet names so we must know the table name
                    # But the original script had a hardcoded one or let the user decide.
                    # Let's enforce it or use filename.
                    target_table = os.path.basename(file_path).split('.')[0]
                    proceed = messagebox.askyesno("Table Name", f"Target table name not specified. Use '{target_table}'?")
                    if not proceed: return

                import_from_pkl(db_config, file_path, target_table)
                messagebox.showinfo("Success", "Import completed.")

        except Exception as e:
            messagebox.showerror("Error", f"An error occurred:\n{str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    app = MySQLHandlerApp(root)
    root.mainloop()
