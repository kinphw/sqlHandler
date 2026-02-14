import os
import tkinter as tk
from tkinter import filedialog, messagebox

from mysql.frommysql.mysql2xlsx import export_to_xlsx
from mysql.frommysql.mysql2pkl import export_to_pkl
from mysql.tomysql.xlsx2mysql import import_from_xlsx as mysql_import_xlsx
from mysql.tomysql.pkl2mysql import import_from_pkl as mysql_import_pkl
from mysql.gui_widgets import create_export_widgets, get_export_params, create_import_widgets, get_import_params
from mysql.services.db_connection import get_db_url_and_config
from mysql.services.collation_service import fetch_server_collations, fetch_table_collation_info


class MySQLController:
    def __init__(self, app):
        self.app = app
        self._db_default_collation = None
        self._collation_update_job = None

    def update_db_info(self):
        prefix = "PROD_" if self.app.var_prod.get() else ""
        host = os.getenv(f"{prefix}MYSQL_HOST")
        port = os.getenv(f"{prefix}MYSQL_PORT")
        user = os.getenv(f"{prefix}MYSQL_USER")
        self.app.lbl_db_info.config(text=f"Connecting to: {user}@{host}:{port}")

    def update_ui(self):
        mode = self.app.var_mode.get()
        self.toggle_query_panel(False)

        if mode in ["mysql2xlsx", "mysql2pkl"]:
            self.app.widgets = create_export_widgets(
                self.app.lb_input_frame, mode, on_query_mode_change=self.toggle_query_panel
            )
        elif mode in ["xlsx2mysql", "pkl2mysql"]:
            self.app.widgets = create_import_widgets(self.app.lb_input_frame, mode)
            self.populate_collation_dropdown()
            self.attach_collation_ui_handlers()
            self.schedule_collation_status_update()

    def toggle_query_panel(self, show):
        if show:
            if not self.app.is_query_panel_visible:
                self.app.mysql_paned_window.add(self.app.mysql_right_panel)
                self.app.is_query_panel_visible = True
        else:
            if self.app.is_query_panel_visible:
                self.app.mysql_paned_window.forget(self.app.mysql_right_panel)
                self.app.is_query_panel_visible = False

    def _get_db_url_and_config(self, silent=False):
        db_name = self.app.entry_db_name.get()
        on_error = messagebox.showerror if not silent else None
        db_url, config, tunnel = get_db_url_and_config(
            db_name,
            self.app.var_prod.get(),
            env_getter=os.getenv,
            silent=silent,
            on_error=on_error,
        )
        if tunnel:
            self.app.tunnel = tunnel
        return db_url, config

    def _close_tunnel(self):
        if hasattr(self.app, 'tunnel') and self.app.tunnel:
            self.app.tunnel.stop()
            self.app.tunnel = None
            print("🔒 SSH Tunnel Closed.")

    def populate_collation_dropdown(self):
        if not hasattr(self.app, 'widgets'):
            return
        if 'cmb_collation' not in self.app.widgets:
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
            self.app.widgets['cmb_collation']['values'] = values

            current = self.app.widgets['var_collation'].get()
            if current not in values:
                self.app.widgets['var_collation'].set("server_default")

            if db_default:
                hint = f"(DB 기본: {db_default})"
            else:
                hint = "(DB 기본: 알 수 없음)"
            if preferred_missing:
                hint += " / uca1400 미표기"
            self.app.widgets['lbl_collation_hint'].config(text=hint, fg="gray")
        else:
            self.app.widgets['lbl_collation_hint'].config(text="(서버 조회 실패: DB 기본값 알 수 없음)", fg="gray")

    def attach_collation_ui_handlers(self):
        if not hasattr(self.app, 'widgets'):
            return
        if 'var_target_table' not in self.app.widgets:
            return

        def _on_change(*_):
            self.schedule_collation_status_update()

        self.app.widgets['var_target_table'].trace_add('write', _on_change)
        self.app.widgets['var_collation'].trace_add('write', _on_change)
        self.app.widgets['var_import_scope'].trace_add('write', _on_change)

    def schedule_collation_status_update(self):
        if not hasattr(self.app, 'root'):
            return
        if self._collation_update_job:
            self.app.root.after_cancel(self._collation_update_job)
        self._collation_update_job = self.app.root.after(300, self.update_collation_status)

    def update_collation_status(self):
        if not hasattr(self.app, 'widgets'):
            return
        widgets = self.app.widgets
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

        db_url, db_config = self._get_db_url_and_config(silent=True)
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

        try:
            table_collation, column_mismatch_count = fetch_table_collation_info(db_config, target_table, selected_effective)
        finally:
            self._close_tunnel()

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

    def fetch_server_collations(self):
        db_url, db_config = self._get_db_url_and_config(silent=True)
        if not db_url or not db_config:
            return None, None
        try:
            return fetch_server_collations(db_config)
        finally:
            self._close_tunnel()

    def run_process(self):
        try:
            db_url, db_config = self._get_db_url_and_config()
            if not db_url:
                return

            mode = self.app.var_mode.get()

            if mode in ["mysql2xlsx", "mysql2pkl"]:
                params = get_export_params(self.app.widgets)
                if params is None:
                    messagebox.showwarning("Warning", "Check input fields.")
                    return

                export_scope = params.get('scope', 'table')
                table_name = params['table_name']
                query = None

                if export_scope == 'query':
                    query = self.app.txt_query.get("1.0", tk.END).strip()
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
                if export_scope == 'query':
                    default_name = "query_result" + ext
                elif table_name:
                    default_name = table_name + ext
                else:
                    default_name = self.app.entry_db_name.get() + "_full" + ext

                save_path = filedialog.asksaveasfilename(
                    defaultextension=ext, filetypes=filetypes, initialfile=default_name
                )
                if not save_path:
                    return

                if mode == "mysql2xlsx":
                    export_to_xlsx(db_url, export_scope, table_name, query, save_path)
                else:
                    export_to_pkl(db_url, export_scope, table_name, query, save_path)

                messagebox.showinfo("Success", f"Export to {save_path} successful.")

            elif mode in ["xlsx2mysql", "pkl2mysql"]:
                params = get_import_params(self.app.widgets)
                if params is None:
                    messagebox.showwarning("Warning", "Select a file.")
                    return

                if params['import_scope'] == "single" and not params['target_table']:
                    messagebox.showwarning("Warning", "Target table name required.")
                    return

                if mode == "xlsx2mysql":
                    mysql_import_xlsx(
                        db_url,
                        params['file_path'],
                        params['import_scope'],
                        params['source_name'],
                        params['target_table'],
                        params['if_exists'],
                        params.get('collation'),
                        params.get('stop_on_mismatch', True)
                    )
                else:
                    mysql_import_pkl(
                        db_config,
                        params['file_path'],
                        params['import_scope'],
                        params['source_name'],
                        params['target_table'],
                        params['if_exists'],
                        params.get('collation'),
                        params.get('stop_on_mismatch', True)
                    )

                messagebox.showinfo("Success", "Import successful.")

        except Exception as e:
            messagebox.showerror("Error", f"An error occurred:\n{str(e)}")
        finally:
            self._close_tunnel()
