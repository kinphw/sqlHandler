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

        # Force initial UI update to show Settings for default selection
        self.update_ui()

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
            self.populate_collation_dropdown()
            self.attach_collation_ui_handlers()
            self.schedule_collation_status_update()

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

    def get_db_url_and_config(self, silent=False):
        db_name = self.entry_db_name.get()
        if not db_name:
            if not silent:
                messagebox.showerror("Error", "Database name is required.")
            return None, None
            
        prefix = "PROD_" if self.var_prod.get() else ""
        
        config = {
            'user': os.getenv(f"{prefix}MYSQL_USER"),
            'password': os.getenv(f"{prefix}MYSQL_PASSWORD"),
            'host': os.getenv(f"{prefix}MYSQL_HOST"),
            'port': int(os.getenv(f"{prefix}MYSQL_PORT", 3306)),
            'database': db_name
        }
        
        # Validation
        if not all(config.values()):
            if not silent:
                messagebox.showerror("Configuration Error", "Missing .env configuration for selected environment.")
            return None, None

        # SSH Tunnel Support
        self.tunnel = None
        if self.var_prod.get() and os.getenv("SSH_HOST"):
            try:
                # Monkey-patch paramiko for compatibility with sshtunnel
                import paramiko
                if not hasattr(paramiko, 'DSSKey'):
                    # Create a dummy class for DSSKey since it's removed in paramiko 3.0+
                    class DSSKey: 
                        pass
                    paramiko.DSSKey = DSSKey

                from sshtunnel import SSHTunnelForwarder
                
                ssh_host = os.getenv("SSH_HOST")
                ssh_user = os.getenv("SSH_USER")
                ssh_password = os.getenv("SSH_PASSWORD")
                ssh_bind_port = int(os.getenv("SSH_BIND_PORT", 13306))
                
                # Create SSH Tunnel
                # Note: remote_bind_address is where the SSH server connects to. 
                # Since SSH server and DB are on the same machine (192.168.0.7), we use 127.0.0.1 here.
                self.tunnel = SSHTunnelForwarder(
                    (ssh_host, 22),
                    ssh_username=ssh_user,
                    ssh_password=ssh_password,
                    remote_bind_address=('127.0.0.1', config['port']),
                    local_bind_address=('127.0.0.1', ssh_bind_port),
                    set_keepalive=10.0  # Keep connection alive
                )
                self.tunnel.start()

                # Wait a bit for the tunnel to establish
                import time
                time.sleep(1.0)
                
                # Update config to use local forwarded port
                config['host'] = '127.0.0.1'
                config['port'] = ssh_bind_port
                print(f"✅ SSH Tunnel Established: 127.0.0.1:{ssh_bind_port} -> {ssh_host} -> DB")
                
            except ImportError:
                print("⚠️ sshtunnel module not found. Skipping SSH tunnel.")
            except Exception as e:
                if not silent:
                    messagebox.showerror("SSH Error", f"Failed to create SSH tunnel: {e}")
                return None, None

        db_url = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}?charset=utf8mb4"
        return db_url, config

    def populate_collation_dropdown(self):
        """Fetch collations from server and update import dropdown."""
        if not hasattr(self, 'widgets'):
            return
        if 'cmb_collation' not in self.widgets:
            return

        collations, db_default = self.fetch_server_collations()
        self._db_default_collation = db_default
        if collations:
            preferred = "utf8mb4_uca1400_ai_ci"
            preferred_missing = preferred not in collations
            if not preferred_missing:
                collations = [preferred] + [c for c in collations if c != preferred]
            values = ["server_default"] + collations
            if preferred_missing:
                values.insert(1, preferred)
            self.widgets['cmb_collation']['values'] = values

            current = self.widgets['var_collation'].get()
            if current not in values:
                self.widgets['var_collation'].set("server_default")

            if db_default:
                hint = f"(DB 기본: {db_default})"
            else:
                hint = "(DB 기본: 알 수 없음)"
            if preferred_missing:
                hint += " / uca1400 미표기"
            self.widgets['lbl_collation_hint'].config(text=hint, fg="gray")
        else:
            self.widgets['lbl_collation_hint'].config(text="(서버 조회 실패: DB 기본값 알 수 없음)", fg="gray")

    def attach_collation_ui_handlers(self):
        if not hasattr(self, 'widgets'):
            return
        if 'var_target_table' not in self.widgets:
            return

        def _on_change(*_):
            self.schedule_collation_status_update()

        self.widgets['var_target_table'].trace_add('write', _on_change)
        self.widgets['var_collation'].trace_add('write', _on_change)
        self.widgets['var_import_scope'].trace_add('write', _on_change)

    def schedule_collation_status_update(self):
        if not hasattr(self, 'root'):
            return
        if hasattr(self, '_collation_update_job') and self._collation_update_job:
            self.root.after_cancel(self._collation_update_job)
        self._collation_update_job = self.root.after(300, self.update_collation_status)

    def update_collation_status(self):
        if not hasattr(self, 'widgets'):
            return
        widgets = self.widgets
        if 'lbl_table_collation_value' not in widgets:
            return

        import_scope = widgets['var_import_scope'].get()
        if import_scope != "single":
            widgets['lbl_table_collation_value'].config(text="전체 모드: 테이블별 표시 없음", fg="gray")
            widgets['lbl_collation_compare_value'].config(text="-", fg="gray")
            return

        target_table = widgets['var_target_table'].get().strip()
        if not target_table:
            widgets['lbl_table_collation_value'].config(text="대상 테이블명 입력 필요", fg="gray")
            widgets['lbl_collation_compare_value'].config(text="-", fg="gray")
            return

        db_url, db_config = self.get_db_url_and_config(silent=True)
        if not db_url or not db_config:
            widgets['lbl_table_collation_value'].config(text="DB 설정 필요", fg="gray")
            widgets['lbl_collation_compare_value'].config(text="-", fg="gray")
            return

        selected = widgets['var_collation'].get()
        if selected == "server_default":
            if self._db_default_collation:
                selected_text = f"server_default ({self._db_default_collation})"
                selected_effective = self._db_default_collation
            else:
                selected_text = "server_default (알 수 없음)"
                selected_effective = None
        else:
            selected_text = selected
            selected_effective = selected

        table_collation, column_mismatch_count = self.fetch_table_collation_info(db_config, target_table, selected_effective)

        if table_collation:
            widgets['lbl_table_collation_value'].config(text=f"{table_collation}", fg="black")
            if selected_effective:
                if table_collation == selected_effective:
                    compare_text = f"일치 (선택: {selected_text})"
                    compare_color = "green"
                else:
                    compare_text = f"불일치 (선택: {selected_text})"
                    compare_color = "red"
            else:
                compare_text = f"비교 불가 (선택: {selected_text})"
                compare_color = "gray"

            if column_mismatch_count:
                compare_text += f" / 컬럼 불일치: {column_mismatch_count}"
            widgets['lbl_collation_compare_value'].config(text=compare_text, fg=compare_color)
        else:
            widgets['lbl_table_collation_value'].config(text="테이블 없음: 신규 생성 예정", fg="gray")
            widgets['lbl_collation_compare_value'].config(text=f"적용 예정: {selected_text}", fg="gray")

    def fetch_table_collation_info(self, db_config, table_name, compare_collation):
        """Return (table_collation, mismatched_column_count)."""
        conn = None
        try:
            conn = pymysql.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                port=int(db_config['port']),
                database=db_config['database'],
                charset='utf8mb4',
                autocommit=True
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT TABLE_COLLATION
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                    """,
                    (db_config['database'], table_name)
                )
                row = cur.fetchone()
                table_collation = row[0] if row else None

                mismatch_count = 0
                if compare_collation:
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                          AND collation_name IS NOT NULL
                          AND collation_name <> %s
                        """,
                        (db_config['database'], table_name, compare_collation)
                    )
                    mismatch_row = cur.fetchone()
                    mismatch_count = mismatch_row[0] if mismatch_row and mismatch_row[0] else 0

            return table_collation, mismatch_count
        except Exception as e:
            print(f"⚠️ 테이블 콜레이션 조회 실패: {e}")
            return None, 0
        finally:
            if conn:
                conn.close()
            if hasattr(self, 'tunnel') and self.tunnel:
                self.tunnel.stop()
                self.tunnel = None
                print("🔒 SSH Tunnel Closed.")

    def fetch_server_collations(self):
        """Return (collations, db_default_collation) or (None, None) on failure."""
        db_url, db_config = self.get_db_url_and_config(silent=True)
        if not db_url or not db_config:
            return None, None

        conn = None
        try:
            conn = pymysql.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                port=int(db_config['port']),
                database=db_config['database'],
                charset='utf8mb4',
                autocommit=True
            )
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DEFAULT_COLLATION_NAME FROM information_schema.schemata WHERE schema_name=%s",
                    (db_config['database'],)
                )
                row = cur.fetchone()
                db_default = row[0] if row else None

                cur.execute(
                    """
                    SELECT COLLATION_NAME
                    FROM information_schema.COLLATIONS
                    WHERE CHARACTER_SET_NAME = 'utf8mb4'
                    ORDER BY COLLATION_NAME
                    """
                )
                rows = cur.fetchall()
                collations = [r[0] for r in rows if r and r[0]]

            return collations, db_default
        except Exception as e:
            print(f"⚠️ Collation 목록 조회 실패: {e}")
            return None, None
        finally:
            if conn:
                conn.close()
            if hasattr(self, 'tunnel') and self.tunnel:
                self.tunnel.stop()
                self.tunnel = None
                print("🔒 SSH Tunnel Closed.")

    def run_process(self):
        """Execute MySQL Process"""
        try:
            db_url, db_config = self.get_db_url_and_config()
            if not db_url: return

            mode = self.var_mode.get()
            
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
                    mysql_import_xlsx(db_url, params['file_path'], params['import_scope'], params['source_name'], params['target_table'], params['if_exists'], params.get('collation'), params.get('stop_on_mismatch', True))
                else:
                    mysql_import_pkl(db_config, params['file_path'], params['import_scope'], params['source_name'], params['target_table'], params['if_exists'], params.get('collation'), params.get('stop_on_mismatch', True))
                
                messagebox.showinfo("Success", "Import successful.")

        except Exception as e:
            messagebox.showerror("Error", f"An error occurred:\n{str(e)}")
        finally:
            # Close SSH Tunnel if it exists
            if hasattr(self, 'tunnel') and self.tunnel:
                self.tunnel.stop()
                print("🔒 SSH Tunnel Closed.")


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
