import tkinter as tk
from tkinter import ttk
import os
import sys
from dotenv import load_dotenv

# Updated import paths for MySQL widgets
from mysql.gui_widgets import MySQLView
from mysql.controller import MySQLController

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
        
        # --- Tab 1: MySQL (MVC Pattern) ---
        self.mysql_view = MySQLView(self.notebook, self)
        self.mysql_controller = MySQLController(self.mysql_view)
        
        self.notebook.add(self.mysql_view.get_tab_frame(), text="MySQL Handler")
        
        # --- Tab 2: SQLite (Existing Function Pattern) ---
        self.tab_sqlite = create_sqlite_tab(self.notebook)
        self.notebook.add(self.tab_sqlite, text="SQLite Handler")
        
        # Note: Global stdout redirection removed to decouple modules.
        # specific modules now handle their own logging.

    def center_window(self):
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')

if __name__ == "__main__":
    root = tk.Tk()
    app = DataHandlerApp(root)
    root.mainloop()
