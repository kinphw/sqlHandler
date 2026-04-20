import tkinter as tk
from tkinter import ttk
from dotenv import load_dotenv

from connection import ConnectionManager, ConnectionView, ConnectionController
from mysql.gui_widgets import MySQLView
from mysql.controller import MySQLController
from sqlite.gui_widgets import create_sqlite_tab
from cleaner import CleanerView, CleanerController

# Load .env
load_dotenv('.env')


class DataHandlerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("SQL Data Handler (MySQL & SQLite)")
        self.root.geometry("1000x1000")

        self.center_window()

        # --- Shared DB connection (used by MySQL Handler & Table Cleaner) ---
        self.conn_mgr = ConnectionManager()

        # --- Main Layout: Notebook (Tabs) ---
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)

        # --- Tab 1: DB 연결 (Connection) ---
        self.conn_view = ConnectionView(self.notebook, self)
        self.conn_ctrl = ConnectionController(self.conn_view, self.conn_mgr)
        self.notebook.add(self.conn_view.get_tab_frame(), text="DB 연결")

        # --- Tab 2: MySQL Handler ---
        self.mysql_view = MySQLView(self.notebook, self)
        self.mysql_controller = MySQLController(self.mysql_view, self.conn_mgr)
        self.notebook.add(self.mysql_view.get_tab_frame(), text="MySQL Handler")

        # --- Tab 3: SQLite Handler ---
        self.tab_sqlite = create_sqlite_tab(self.notebook)
        self.notebook.add(self.tab_sqlite, text="SQLite Handler")

        # --- Tab 4: Table Cleaner ---
        self.cleaner_view = CleanerView(self.notebook, self)
        self.cleaner_controller = CleanerController(self.cleaner_view, self.conn_mgr)
        self.notebook.add(self.cleaner_view.get_tab_frame(), text="Table Cleaner")

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
