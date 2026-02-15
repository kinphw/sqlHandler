import os
import pickle
import threading
import pandas as pd
from mysql.frommysql.mysql2xlsx import export_to_xlsx
from mysql.frommysql.mysql2pkl import export_to_pkl
from mysql.tomysql.xlsx2mysql import import_from_xlsx as mysql_import_xlsx
from mysql.tomysql.pkl2mysql import import_from_pkl as mysql_import_pkl
from mysql.services.db_connection import get_db_url_and_config
from mysql.services.collation_service import fetch_server_collations, fetch_table_collation_info
from mysql.services.column_service import fetch_table_columns

class MySQLController:
    def __init__(self, view):
        self.view = view
        self._db_default_collation = None
        self._collation_update_job = None
        self.tunnel = None
        self._cached_source_columns = {}  # {source_name: [col1, col2, ...]}
        self._import_context = None  # stores state during comparison wizard
        
        # Bind Events
        self.view.bind_event('run_button', self.run_process)
        self.view.bind_event('release_button', self.release_all)
        self.view.bind_event('db_prod_change', self.update_db_info)
        self.view.bind_event('mode_change', self.on_mode_change)
        
        # Initial Setup
        self.update_db_info()
        # Initialize UI widgets first so variables are bound
        self.update_ui()

    def update_db_info(self, *args):
        prefix = "PROD_" if self.view.get_prod_checked() else ""
        host = os.getenv(f"{prefix}MYSQL_HOST")
        port = os.getenv(f"{prefix}MYSQL_PORT")
        user = os.getenv(f"{prefix}MYSQL_USER")
        self.view.set_db_info_label(f"Connecting to: {user}@{host}:{port}")

    def on_mode_change(self, *args):
        self.update_ui()

    def update_ui(self):
        mode = self.view.get_mode()
        self.view.toggle_query_panel(False)
        self.view.hide_comparison_panel()
        self._import_context = None
        
        def on_query_mode_change(is_query_mode):
            self.view.toggle_query_panel(is_query_mode)

        self.view.update_input_widgets(mode, on_query_mode_change)
        
        if mode in ["xlsx2mysql", "pkl2mysql"]:
            self.view.set_on_file_selected(self.on_file_selected)
            self.populate_collation_dropdown()
            self.attach_collation_ui_handlers()
            self.schedule_collation_status_update()

    def _get_db_url_and_config(self, silent=False):
        db_name = self.view.get_db_name()
        on_error = self.view.show_error if not silent else None
        
        db_url, config, tunnel = get_db_url_and_config(
            db_name,
            self.view.get_prod_checked(),
            env_getter=os.getenv,
            silent=silent,
            on_error=on_error,
        )
        if tunnel:
            self.tunnel = tunnel
        return db_url, config

    def _close_tunnel(self):
        if self.tunnel:
            self.tunnel.stop()
            self.tunnel = None
            print("🔒 SSH Tunnel Closed.")

    def release_all(self):
        """모든 파일 핸들, DB 연결, 캐시를 해제"""
        self._close_tunnel()
        self._cached_source_columns = {}
        self._import_context = None
        self.view.hide_comparison_panel()

        import gc
        gc.collect()

        self.view.log("[Release] 모든 리소스가 해제되었습니다.")

    @staticmethod
    def _normalize_columns(columns):
        """Normalize column names the same way import functions do."""
        return [col.strip().replace(" ", "_").lower() for col in columns]

    def on_file_selected(self, filepath, mode):
        """Inspect selected file and populate source dropdown with keys/sheets. Also cache column info."""
        self._cached_source_columns = {}
        if not filepath or not os.path.isfile(filepath):
            self.view.update_source_dropdown([], None)
            return

        try:
            if mode == "pkl2mysql":
                with open(filepath, 'rb') as f:
                    data = pickle.load(f)
                if isinstance(data, dict):
                    keys = list(data.keys())
                    help_text = f"(Dictionary: {len(keys)}개 키)"
                    self.view.update_source_dropdown([str(k) for k in keys], help_text)
                    # Cache columns for each key
                    for k, v in data.items():
                        if isinstance(v, pd.DataFrame):
                            self._cached_source_columns[str(k)] = self._normalize_columns(v.columns.tolist())
                elif isinstance(data, pd.DataFrame):
                    help_text = "(단일 DataFrame)"
                    self.view.update_source_dropdown([], help_text)
                    # Cache with filename as key
                    table_name = os.path.basename(filepath).split('.')[0]
                    self._cached_source_columns[table_name] = self._normalize_columns(data.columns.tolist())
                else:
                    help_text = f"(타입: {type(data).__name__})"
                    self.view.update_source_dropdown([], help_text)

            elif mode == "xlsx2mysql":
                with pd.ExcelFile(filepath) as xls:
                    sheets = xls.sheet_names
                help_text = f"(시트: {len(sheets)}개, 비워두면 첫 시트)"
                self.view.update_source_dropdown(sheets, help_text)
                # Cache columns for each sheet (headers only)
                headers = pd.read_excel(filepath, sheet_name=None, nrows=0)
                for sheet_name, df in headers.items():
                    self._cached_source_columns[sheet_name] = self._normalize_columns(df.columns.tolist())

        except Exception as e:
            self.view.log(f"[WARN] 파일 분석 실패: {e}")
            self.view.update_source_dropdown([], "(파일 읽기 실패)")
            return

        # Show comparison panel immediately after file selection
        self._refresh_comparison_preview()

    def populate_collation_dropdown(self):
        try:
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
                
                self.view.update_collation_dropdown(values)

                current = self.view.get_collation_current()
                if current not in values:
                    self.view.set_collation_current("server_default")

                if db_default:
                    hint = f"(DB 기본: {db_default})"
                else:
                    hint = "(DB 기본: 알 수 없음)"
                if preferred_missing:
                    hint += " / uca1400 미표기"
                self.view.set_collation_hint(hint)
            else:
                self.view.set_collation_hint("(서버 조회 실패: DB 기본값 알 수 없음)")
        except Exception:
            self.view.set_collation_hint("(서버 조회 실패)")

    def attach_collation_ui_handlers(self):
        def _on_change(*_):
            self.schedule_collation_status_update()

        self.view.bind_event('target_table_change', _on_change)
        self.view.bind_event('collation_change', _on_change)
        self.view.bind_event('import_scope_change', _on_change)

    def schedule_collation_status_update(self):
        if self._collation_update_job:
            self.view.tab.after_cancel(self._collation_update_job)
        self._collation_update_job = self.view.tab.after(300, self.update_collation_status)

    def update_collation_status(self):
        try:
            # Use lenient getter for UI updates (doesn't require file selected)
            # This fixes the issue where collation info wouldn't show until file was picked
            params = self.view.get_target_table_info()
            
            import_scope = params['import_scope']
            if import_scope != "single":
                self.view.set_table_collation_info("전체 모드: 테이블별 표시 없음", "-", "gray")
                return

            target_table = params['target_table']
            if not target_table:
                self.view.set_table_collation_info("대상 테이블명 입력 필요", "-", "gray")
                return

            db_url, db_config = self._get_db_url_and_config(silent=True)
            if not db_url or not db_config:
                self.view.set_table_collation_info("DB 설정 필요", "-", "gray")
                return

            selected = params['collation']
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
                table_coll_text = f"{table_collation}"
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
                
                self.view.set_table_collation_info(table_coll_text, compare_text, compare_color)
            else:
                self.view.set_table_collation_info(f"테이블 없음: 신규 생성 예정 ({target_table})", f"적용 예정: {selected_text}", "gray")

        except Exception as e:
            # Check if self.view.log exists before calling it? (It should)
            # Log to view for debugging
            self.view.log(f"[DEBUG] Collation check error: {e}")
            print(f"Error in update_collation_status: {e}")

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

            mode = self.view.get_mode()
            self.view.log(f"--- Starting Process: {mode} ---")

            if mode in ["mysql2xlsx", "mysql2pkl"]:
                params = self.view.get_export_params()
                if params is None:
                    self.view.show_warning("Warning", "Check input fields.")
                    return

                export_scope = params.get('scope', 'table')
                table_name = params['table_name']
                query = None

                if export_scope == 'query':
                    query = self.view.get_query_text()
                    if not query:
                        self.view.show_warning("Warning", "Enter a query.")
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
                    default_name = self.view.get_db_name() + "_full" + ext

                from tkinter import filedialog
                save_path = filedialog.asksaveasfilename(
                    defaultextension=ext, filetypes=filetypes, initialfile=default_name
                )
                if not save_path:
                    return

                self.view.log(f"Exporting to: {save_path}")
                
                if mode == "mysql2xlsx":
                    export_to_xlsx(db_url, export_scope, table_name, query, save_path)
                else:
                    export_to_pkl(db_url, export_scope, table_name, query, save_path)

                self.view.log("Export Successful.")
                self.view.show_info("Success", f"Export to {save_path} successful.")

            elif mode in ["xlsx2mysql", "pkl2mysql"]:
                params = self.view.get_import_params()
                if params is None:
                    self.view.show_warning("Warning", "Select a file.")
                    return

                if params['import_scope'] == "single" and not params['target_table']:
                    self.view.show_warning("Warning", "Target table name required.")
                    return

                self.view.log(f"Importing from: {params['file_path']}")

                # If comparison panel is already showing (from file selection preview),
                # collect excluded columns and execute import directly
                if self.view.is_comparison_panel_visible and self._import_context:
                    excluded = self.view.get_excluded_columns()
                    ctx = self._import_context
                    comp = ctx['comparisons'][ctx['current_index']]
                    if excluded:
                        ctx['excluded_columns'][comp['target_table']] = excluded

                    # For multi-table: check if more tables need review
                    if ctx['current_index'] + 1 < len(ctx['comparisons']):
                        ctx['params'] = params
                        ctx['db_url'] = db_url
                        ctx['db_config'] = db_config
                        ctx['current_index'] += 1
                        self._show_next_comparison()
                        return
                    else:
                        ctx['params'] = params
                        ctx['db_url'] = db_url
                        ctx['db_config'] = db_config
                        self.view.hide_comparison_panel()
                        self._execute_import()
                        return
                else:
                    # No preview shown yet - start comparison wizard
                    self._start_import_comparison(db_url, db_config, params, mode)
                    return

        except Exception as e:
            self.view.log(f"Error: {str(e)}")
            self.view.show_error("Error", f"An error occurred:\n{str(e)}")
        finally:
            self._close_tunnel()

    # --- Import Comparison Flow ---

    def _refresh_comparison_preview(self):
        """Show comparison panel immediately after file selection (preview mode).
        For 'all' mode: shows first table comparison right away.
        For 'single' mode: shows if target table name is already entered.
        """
        if not self._cached_source_columns:
            self.view.hide_comparison_panel()
            return

        try:
            db_url, db_config = self._get_db_url_and_config(silent=True)
            if not db_url or not db_config:
                return

            import_scope = self.view.widgets['var_import_scope'].get()
            comparisons = []

            if import_scope == "single":
                target = self.view.widgets['var_target_table'].get().strip()
                if not target:
                    # No target table yet - just show DataFrame columns without MySQL comparison
                    df_cols = list(self._cached_source_columns.values())[0] if self._cached_source_columns else []
                    if df_cols:
                        self.view.show_comparison_panel(
                            table_name="(대상 테이블명 미입력)",
                            df_columns=df_cols,
                            mysql_columns=None,
                            table_index=0,
                            total_tables=1,
                            on_confirm=None,
                            on_refresh=self._refresh_comparison_preview,
                        )
                    return

                source_name = self.view.widgets['var_source_name'].get().strip()
                file_path = self.view.widgets['entry_file_path'].get().strip()
                df_columns = self._find_cached_columns(source_name, target, file_path)
                mysql_columns = fetch_table_columns(db_config, target)
                self._close_tunnel()
                comparisons.append({
                    'target_table': target,
                    'df_columns': df_columns,
                    'mysql_columns': mysql_columns,
                })
            else:
                for source_name, df_cols in self._cached_source_columns.items():
                    target = source_name.strip().lower().replace(" ", "_")
                    mysql_columns = fetch_table_columns(db_config, target)
                    self._close_tunnel()
                    comparisons.append({
                        'target_table': target,
                        'df_columns': df_cols,
                        'mysql_columns': mysql_columns,
                    })

            if not comparisons:
                return

            # Store context for later use by RUN
            self._import_context = {
                'db_url': db_url,
                'db_config': db_config,
                'params': None,  # will be filled on RUN
                'mode': self.view.get_mode(),
                'comparisons': comparisons,
                'current_index': 0,
                'excluded_columns': {},
            }

            # Show first table comparison (preview mode - no confirm button, RUN handles import)
            comp = comparisons[0]
            self.view.show_comparison_panel(
                table_name=comp['target_table'],
                df_columns=comp['df_columns'],
                mysql_columns=comp['mysql_columns'],
                table_index=0,
                total_tables=len(comparisons),
                on_confirm=None,
                on_refresh=self._refresh_comparison_preview,
            )

        except Exception as e:
            self.view.log(f"[DEBUG] Comparison preview error: {e}")
            self._close_tunnel()

    def _start_import_comparison(self, db_url, db_config, params, mode):
        """Build comparison data for all tables and start the wizard."""
        try:
            comparisons = []

            if params['import_scope'] == "single":
                # Single table
                target = params['target_table']
                source_name = params['source_name']

                # Find cached columns
                df_columns = self._find_cached_columns(source_name, target, params['file_path'])

                mysql_columns = fetch_table_columns(db_config, target)
                self._close_tunnel()

                comparisons.append({
                    'target_table': target,
                    'df_columns': df_columns,
                    'mysql_columns': mysql_columns,
                })
            else:
                # All tables - use cached source columns
                if not self._cached_source_columns:
                    self.view.show_warning("Warning", "파일을 먼저 선택해주세요 (Browse).")
                    return

                for source_name, df_cols in self._cached_source_columns.items():
                    target = source_name.strip().lower().replace(" ", "_")
                    mysql_columns = fetch_table_columns(db_config, target)
                    self._close_tunnel()

                    comparisons.append({
                        'target_table': target,
                        'df_columns': df_cols,
                        'mysql_columns': mysql_columns,
                    })

            if not comparisons:
                self.view.show_warning("Warning", "비교할 테이블이 없습니다.")
                return

            self._import_context = {
                'db_url': db_url,
                'db_config': db_config,
                'params': params,
                'mode': mode,
                'comparisons': comparisons,
                'current_index': 0,
                'excluded_columns': {},  # {target_table: [excluded_col_names]}
            }
            self._show_next_comparison()

        except Exception as e:
            self.view.log(f"Error during comparison setup: {e}")
            self.view.show_error("Error", f"비교 준비 중 오류:\n{e}")
            self._close_tunnel()

    def _find_cached_columns(self, source_name, target_table, file_path):
        """Find DataFrame columns from cache for a given source."""
        if source_name and source_name in self._cached_source_columns:
            return self._cached_source_columns[source_name]
        if target_table and target_table in self._cached_source_columns:
            return self._cached_source_columns[target_table]
        # Fallback: try filename
        basename = os.path.basename(file_path).split('.')[0]
        if basename in self._cached_source_columns:
            return self._cached_source_columns[basename]
        # If cache is single entry, use it
        if len(self._cached_source_columns) == 1:
            return list(self._cached_source_columns.values())[0]
        return []

    def _show_next_comparison(self):
        """Show comparison panel for the current table index."""
        ctx = self._import_context
        idx = ctx['current_index']
        comp = ctx['comparisons'][idx]

        self.view.show_comparison_panel(
            table_name=comp['target_table'],
            df_columns=comp['df_columns'],
            mysql_columns=comp['mysql_columns'],
            table_index=idx,
            total_tables=len(ctx['comparisons']),
            on_confirm=self._on_comparison_confirm,
            on_refresh=self._refresh_comparison_preview,
        )

    def _on_comparison_confirm(self):
        """User confirmed current table - save excluded columns and proceed."""
        ctx = self._import_context
        idx = ctx['current_index']
        comp = ctx['comparisons'][idx]

        excluded = self.view.get_excluded_columns()
        if excluded:
            ctx['excluded_columns'][comp['target_table']] = excluded

        if idx + 1 < len(ctx['comparisons']):
            ctx['current_index'] = idx + 1
            self._show_next_comparison()
        else:
            # All tables reviewed - proceed with import
            self.view.hide_comparison_panel()
            self._execute_import()

    def _execute_import(self):
        """Run the actual import with excluded columns applied."""
        ctx = self._import_context
        if not ctx:
            return

        try:
            db_url = ctx['db_url']
            db_config = ctx['db_config']
            params = ctx['params']
            mode = ctx['mode']
            excluded = ctx['excluded_columns'] if ctx['excluded_columns'] else None

            if excluded:
                for tbl, cols in excluded.items():
                    self.view.log(f"  제외 컬럼 ({tbl}): {', '.join(cols)}")

            if mode == "xlsx2mysql":
                mysql_import_xlsx(
                    db_url,
                    params['file_path'],
                    params['import_scope'],
                    params['source_name'],
                    params['target_table'],
                    params['if_exists'],
                    params.get('collation'),
                    params.get('stop_on_mismatch', True),
                    excluded_columns=excluded,
                    logger=self.view.log
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
                    params.get('stop_on_mismatch', True),
                    excluded_columns=excluded,
                    logger=self.view.log
                )

            self.view.log("Import Successful.")
            self.view.show_info("Success", "Import successful.")

        except Exception as e:
            self.view.log(f"Error: {str(e)}")
            self.view.show_error("Error", f"An error occurred:\n{str(e)}")
        finally:
            self._close_tunnel()
            self._import_context = None
            # Import 후 비교 패널 복원 (연속 Import 지원)
            if self._cached_source_columns:
                self._refresh_comparison_preview()
