import tkinter as tk
from tkinter import ttk
import os
import sys
from dotenv import load_dotenv

# Updated import paths for MySQL widgets
from mysql.gui_widgets import create_mysql_tab
from mysql.controller import MySQLController
from ui.log_redirect import TextRedirector

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
        
        # MySQL controller
        self.mysql_controller = MySQLController(self)

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

        # Force initial UI update to show Settings for default selection
        self.update_ui()

    def center_window(self):
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')

    # --- MySQL Logic Methods (delegated to controller) ---

    def update_db_info(self):
        return self.mysql_controller.update_db_info()

    def update_ui(self):
        return self.mysql_controller.update_ui()

    def toggle_query_panel(self, show):
        return self.mysql_controller.toggle_query_panel(show)

    def run_process(self):
        return self.mysql_controller.run_process()


if __name__ == "__main__":
    root = tk.Tk()
    app = DataHandlerApp(root)
    root.mainloop()
