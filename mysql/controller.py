import os
import pickle
import threading
import pandas as pd
from mysql.frommysql.mysql2xlsx import export_to_xlsx
from mysql.frommysql.mysql2pkl import export_to_pkl
from mysql.tomysql.xlsx2mysql import import_from_xlsx as mysql_import_xlsx
from mysql.tomysql.pkl2mysql import import_from_pkl as mysql_import_pkl
from mysql.services.collation_service import fetch_server_collations, fetch_table_collation_info
from mysql.services.column_service import fetch_table_columns
from mysql.services.query_safety import validate_read_only_query
from mysql.services.data_stats_service import (
    fetch_table_row_count,
    fetch_all_table_row_counts,
    fetch_query_row_count,
    fetch_key_columns,
    count_existing_keys,
    fetch_table_sample,
    fetch_query_sample,
    fetch_existing_key_set,
)
from mysql.preview import open_data_preview

PREVIEW_ROW_LIMIT = 500  # 미리보기 창에 표시할 최대 행 수


class MySQLController:
    def __init__(self, view, connection_manager):
        self.view = view
        self._conn_mgr = connection_manager
        self._db_default_collation = None
        self._collation_update_job = None
        self._cached_source_columns = {}  # {source_name: [col1, col2, ...]}
        self._import_context = None  # stores state during comparison wizard
        self._validation_keys = {}  # {target_table: [key columns]} - Test 미리보기 검증 키

        # Bind Events
        self.view.bind_event('run_button', self.run_process)
        self.view.bind_event('test_button', self.run_test)
        self.view.bind_event('release_button', self.release_all)
        self.view.bind_event('mode_change', self.on_mode_change)

        # Initial Setup
        self.update_db_info()
        # Initialize UI widgets first so variables are bound
        self.update_ui()

    def update_db_info(self, *args):
        if self._conn_mgr.is_connected():
            m = self._conn_mgr
            self.view.set_db_info_label(
                f"연결됨: {m.user}@{m._eff_host}:{m._eff_port}/{m.db_name}"
            )
        else:
            self.view.set_db_info_label("연결 없음 — 'DB 연결' 탭에서 연결하세요")

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
        if not self._conn_mgr.is_connected():
            if not silent:
                self.view.show_error(
                    "연결 오류",
                    "DB에 연결되어 있지 않습니다.\n'DB 연결' 탭에서 먼저 연결하세요."
                )
            return None, None
        return self._conn_mgr.get_db_url(), self._conn_mgr.get_config()

    def _close_tunnel(self):
        pass  # Tunnel is managed by ConnectionManager — do not close here

    def release_all(self):
        """파일 핸들·캐시·비교 패널 해제 (DB 연결은 ConnectionManager가 관리)"""
        self._cached_source_columns = {}
        self._import_context = None
        self._validation_keys = {}
        self.view.hide_comparison_panel()

        import gc
        gc.collect()

        self.view.log("[Release] 캐시가 해제되었습니다.")

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
        self.view.bind_event('import_mode_change', _on_change)

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

            is_append = params.get('if_exists') == "append"
            if table_collation:
                table_coll_text = f"{table_collation}"
                if selected_effective:
                    if table_collation == selected_effective:
                        compare_text = f"일치 (선택: {selected_text})"
                        compare_color = "green"
                    elif is_append:
                        # Append는 기존 콜레이션을 유지하므로 불일치는 참고용
                        compare_text = f"Append: 기존 콜레이션 유지 (선택 {selected_text}는 참고용)"
                        compare_color = "#CC6600"
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
                    validate_read_only_query(query)

                # --- 사전 행 수 체크 (execute 전) ---
                if not self._export_precheck_confirm(db_config, export_scope, table_name, query):
                    self.view.log("[Export] 사전 체크 단계에서 중단되었습니다.")
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
                    default_name = self._conn_mgr.db_name + "_full" + ext

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

    # --- Pre-check Flow (execute 전 사전 점검 / Test 버튼 공용) ---

    def _collect_export_precheck(self, db_config, export_scope, table_name, query):
        """대상 행 수를 조회해 (body, warn)를 반환한다.
        warn이 있으면 진행 불가(대상 없음 등), body는 표시용 본문."""
        if export_scope == "table":
            count = fetch_table_row_count(db_config, table_name)
            if count is None:
                return None, f"테이블 '{table_name}'을(를) 찾을 수 없습니다."
            self.view.log(f"[사전 체크] 테이블 '{table_name}': {count:,} rows")
            return f"테이블 '{table_name}'\n추출 대상: {count:,} rows", None

        if export_scope == "database":
            counts = fetch_all_table_row_counts(db_config)
            if not counts:
                return None, "데이터베이스에 테이블이 없습니다."
            total = sum(c for _, c in counts if c >= 0)
            lines = [
                f"  - {t}: {c:,} rows" if c >= 0 else f"  - {t}: (조회 실패)"
                for t, c in counts
            ]
            self.view.log(f"[사전 체크] 전체 DB: {len(counts)}개 테이블, 총 {total:,} rows")
            for ln in lines:
                self.view.log(ln)
            preview = "\n".join(lines[:20])
            if len(lines) > 20:
                preview += f"\n  ... 외 {len(lines) - 20}개 테이블 (전체는 로그 참조)"
            return f"전체 데이터베이스: {len(counts)}개 테이블, 총 {total:,} rows\n\n{preview}", None

        if export_scope == "query":
            count = fetch_query_row_count(db_config, query)
            if count is None:
                self.view.log("[사전 체크] 쿼리 결과 행 수를 미리 확인할 수 없습니다.")
                return "쿼리 결과 행 수를 미리 확인할 수 없습니다.", None
            self.view.log(f"[사전 체크] 쿼리 결과: {count:,} rows")
            return f"쿼리 결과 추출 대상: {count:,} rows", None

        return "", None

    def _open_export_preview(self, db_config, export_scope, table_name, query):
        """Export 대상 데이터를 별도 미리보기 창으로 띄운다 (table / query 스코프)."""
        root = self.view.tab.winfo_toplevel()
        if export_scope == "table":
            total = fetch_table_row_count(db_config, table_name)
            if total is None:
                self.view.show_warning("미리보기", f"테이블 '{table_name}'을(를) 찾을 수 없습니다.")
                return
            sample = fetch_table_sample(db_config, table_name, PREVIEW_ROW_LIMIT)
            if sample is None:
                self.view.show_warning("미리보기", "데이터를 불러오지 못했습니다.")
                return
            columns, rows = sample
            self.view.log(f"[미리보기] 테이블 '{table_name}': 전체 {total:,} rows, 상위 {len(rows):,} rows 표시")
            tbl = {
                'name': table_name,
                'columns': columns,
                'rows': rows,
                'total_rows': total,
                'status_per_row': None,
                'summary': self._sample_summary(total, len(rows)),
            }
            open_data_preview(root, f"Export 미리보기 — {table_name}", [tbl])

        elif export_scope == "query":
            total = fetch_query_row_count(db_config, query)
            sample = fetch_query_sample(db_config, query, PREVIEW_ROW_LIMIT)
            if sample is None:
                self.view.show_warning("미리보기", "쿼리 결과를 불러오지 못했습니다.")
                return
            columns, rows = sample
            shown = len(rows)
            if total is not None:
                self.view.log(f"[미리보기] 쿼리 결과: 전체 {total:,} rows, 상위 {shown:,} rows 표시")
                summary = self._sample_summary(total, shown)
            else:
                self.view.log(f"[미리보기] 쿼리 결과: 상위 {shown:,} rows 표시 (전체 행수 확인 불가)")
                summary = f"전체 행수 확인 불가 / 미리보기 상위 {shown:,} rows"
            tbl = {
                'name': 'query_result',
                'columns': columns,
                'rows': rows,
                'total_rows': total if total is not None else shown,
                'status_per_row': None,
                'summary': summary,
            }
            open_data_preview(root, "Export 미리보기 — 쿼리 결과", [tbl])

    @staticmethod
    def _sample_summary(total, shown):
        if shown < total:
            return f"전체 {total:,} rows / 미리보기 상위 {shown:,} rows"
        return f"전체 {total:,} rows (전부 표시)"

    def _collect_validation_keys(self):
        """비교 패널 우측에서 현재 표시 테이블의 검증 키 선택을 읽어 누적 저장한다.
        반환: {target_table: [key columns]} (Test 미리보기 전용)."""
        if self.view.is_comparison_panel_visible and self._import_context:
            comps = self._import_context.get('comparisons') or []
            cidx = self._import_context.get('current_index', 0)
            if 0 <= cidx < len(comps):
                cur_table = comps[cidx]['target_table']
                selected = self.view.get_validation_key_columns()
                if selected:
                    self._validation_keys[cur_table] = selected
        return dict(self._validation_keys)

    def _build_import_preview_tables(self, ctx, stats, validation_keys=None):
        """Import 대상 파일을 읽어 테이블별 미리보기 데이터(+행별 기적재 검증)를 만든다.
        validation_keys: {target_table: [사용자가 선택한 검증 키 컬럼]}."""
        validation_keys = validation_keys or {}
        params = ctx['params']
        mode = ctx['mode']
        db_config = ctx['db_config']

        tables_data = self._load_source_tables(
            mode, params['file_path'], params['import_scope'], params['source_name'], ctx
        )
        stats_by_table = {s['table']: s for s in stats}

        result = []
        for comp in ctx['comparisons']:
            target = comp['target_table']
            df = tables_data.get(target)
            if df is None:
                continue

            total = len(df)
            columns = [str(c) for c in df.columns]
            norm_cols = [c.strip().replace(" ", "_").lower() for c in columns]
            sample_df = df.head(PREVIEW_ROW_LIMIT)
            rows = [tuple(r) for r in sample_df.itertuples(index=False, name=None)]

            st = stats_by_table.get(target)
            db_exists = bool(st and st.get('db_rows') is not None)
            selected_keys = validation_keys.get(target)

            status_per_row = None
            if selected_keys and db_exists:
                status_per_row = self._compute_row_status(db_config, target, selected_keys, norm_cols, rows)

            result.append({
                'name': target,
                'columns': columns,
                'rows': rows,
                'total_rows': total,
                'status_per_row': status_per_row,
                'summary': self._import_sample_summary(st, total, len(rows), selected_keys, status_per_row),
            })
        return result

    def _compute_row_status(self, db_config, target, key_columns, norm_cols, rows):
        """선택한 검증 키(key_columns)로 표시 행마다 '기적재'/'신규'/'확인불가'를 매긴다.
        키 컬럼이 파일에 없으면 None(검증 불가)."""
        if not key_columns:
            return None
        norm_key = [k.strip().replace(" ", "_").lower() for k in key_columns]
        if not all(k in norm_cols for k in norm_key):
            return None

        col_idx = {c: i for i, c in enumerate(norm_cols)}
        key_pos = [col_idx[k] for k in norm_key]

        row_keys = []  # 행별 키 튜플 또는 None(키에 NULL 포함 시)
        for r in rows:
            kt = []
            null = False
            for p in key_pos:
                v = self._py_val(r[p])
                if v is None:
                    null = True
                    break
                kt.append(v)
            row_keys.append(None if null else tuple(kt))

        distinct = [k for k in row_keys if k is not None]
        exist_set = fetch_existing_key_set(db_config, target, key_columns, distinct)

        status = []
        for k in row_keys:
            if k is None:
                status.append("확인불가")
            elif k in exist_set:
                status.append("기적재")
            else:
                status.append("신규")
        return status

    @staticmethod
    def _py_val(v):
        if pd.api.types.is_scalar(v) and pd.isna(v):
            return None
        return v.item() if hasattr(v, "item") else v

    @staticmethod
    def _import_sample_summary(st, total, shown, selected_keys=None, status_per_row=None):
        base = (f"파일 전체 {total:,} rows / 미리보기 상위 {shown:,} rows"
                if shown < total else f"파일 전체 {total:,} rows (전부 표시)")
        if st and st.get('db_rows') is None:
            return base + " / DB 신규 테이블 (기적재 없음)"
        if st and st.get('db_rows') is not None:
            base += f" / DB 기존 {st['db_rows']:,} rows"

        if not selected_keys:
            base += " / 검증 키 미선택 (비교 패널 우측에서 키 체크 후 Test)"
        elif status_per_row is None:
            base += f" / 선택 키({'+'.join(selected_keys)})가 파일에 없어 검증 불가"
        else:
            dup = sum(1 for s in status_per_row if s == "기적재")
            base += f" / 검증 키: {'+'.join(selected_keys)} → (미리보기) 기적재 {dup:,}건"
        return base

    def _export_precheck_confirm(self, db_config, export_scope, table_name, query):
        """Export 실행 전 대상 행 수를 조회하고 사용자 확인을 받는다.
        진행하면 True, 중단(취소/대상없음)이면 False."""
        try:
            body, warn = self._collect_export_precheck(db_config, export_scope, table_name, query)
        except Exception as e:
            self.view.log(f"[사전 체크] 오류: {e}")
            return self.view.show_confirm(
                "Export 사전 체크",
                f"사전 체크 중 오류가 발생했습니다:\n{e}\n\n그래도 계속하시겠습니까?"
            )

        if warn:
            self.view.show_warning("사전 체크", warn)
            return False
        return self.view.show_confirm("Export 사전 체크", f"{body}\n\n계속하시겠습니까?")

    def _format_import_stats(self, stats, if_exists):
        """Import 사전 통계 리스트를 사람이 읽는 본문 문자열로 만든다 (로그도 함께 남김)."""
        mode_text = "Replace(대체)" if if_exists == "replace" else "Append(추가)"
        lines = []
        total_dup = 0
        for s in stats:
            line = f"• {s['table']}: 파일 {s['file_rows']:,} rows"
            if s['db_rows'] is None:
                line += " / DB 신규 테이블"
            else:
                line += f" / DB 기존 {s['db_rows']:,} rows"
                if not s['key_columns']:
                    line += " / 키 없음(중복 판별 불가)"
                elif s['dup_rows'] is None:
                    line += " / 중복 확인 불가"
                else:
                    src = " · 선택 키" if s.get('key_source') == 'user' else ""
                    line += f" / 중복(키 {'+'.join(s['key_columns'])}{src}) {s['dup_rows']:,}건"
                    total_dup += s['dup_rows']
            lines.append(line)
            self.view.log(f"[사전 체크] {line}")

        summary = "\n".join(lines)
        note = ""
        if if_exists == "append" and total_dup > 0:
            note = f"\n\n⚠️ Append 모드: 중복 {total_dup:,}건은 INSERT IGNORE로 건너뜁니다."
        elif if_exists == "replace":
            note = "\n\n⚠️ Replace 모드: 기존 데이터를 삭제한 뒤 교체합니다."
        return f"[{mode_text}] 대상 요약:\n\n{summary}{note}"

    def run_test(self):
        """Test 버튼: 실제 Export/Import를 실행하지 않고 사전 체크 결과만 보여준다."""
        try:
            db_url, db_config = self._get_db_url_and_config()
            if not db_url:
                return

            mode = self.view.get_mode()
            self.view.log(f"--- Test (사전 체크): {mode} ---")

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
                    validate_read_only_query(query)

                if export_scope == "database":
                    # 전체 DB는 테이블별 행 수 요약으로 (데이터 미리보기는 생략)
                    body, warn = self._collect_export_precheck(db_config, export_scope, table_name, query)
                    if warn:
                        self.view.show_warning("Export 사전 체크 (Test)", warn)
                    else:
                        self.view.show_info("Export 사전 체크 (Test)", body)
                else:
                    # 특정 테이블 / 사용자 정의 쿼리 → 별도 미리보기 창
                    self._open_export_preview(db_config, export_scope, table_name, query)

            elif mode in ["xlsx2mysql", "pkl2mysql"]:
                params = self.view.get_import_params()
                if params is None:
                    self.view.show_warning("Warning", "Select a file.")
                    return
                if params['import_scope'] == "single" and not params['target_table']:
                    self.view.show_warning("Warning", "Target table name required.")
                    return

                comparisons = self._build_comparisons(db_config, params)
                if not comparisons:
                    self.view.show_warning("Test", "비교할 테이블이 없습니다. 파일을 먼저 선택하세요 (Browse).")
                    return

                # 검증 키(Test 전용): 비교 패널 우측에서 사용자가 체크한 컬럼
                validation_keys = self._collect_validation_keys()

                ctx = {
                    'db_url': db_url,
                    'db_config': db_config,
                    'params': params,
                    'mode': mode,
                    'comparisons': comparisons,
                    'current_index': 0,
                    'excluded_columns': {},
                }
                stats = self._compute_import_stats(ctx)
                preview_tables = self._build_import_preview_tables(ctx, stats, validation_keys)
                if preview_tables:
                    self._format_import_stats(stats, params['if_exists'])  # 로그 기록 (PK 기준 요약)
                    open_data_preview(
                        self.view.tab.winfo_toplevel(),
                        "Import 미리보기 (기적재 검증 포함)",
                        preview_tables,
                    )
                elif stats:
                    self.view.show_info("Import 사전 체크 (Test)", self._format_import_stats(stats, params['if_exists']))
                else:
                    self.view.show_warning("Test", "표시할 내용이 없습니다. 파일/대상 설정을 확인하세요.")

        except Exception as e:
            self.view.log(f"Error: {str(e)}")
            self.view.show_error("Error", f"An error occurred:\n{str(e)}")
        finally:
            self._close_tunnel()

    def _import_precheck_confirm(self, ctx):
        """Import 실행 전 파일 내용·DB 현황·중복을 요약하고 확인을 받는다.
        진행하면 True, 취소면 False."""
        # 비교 패널에서 사용자가 찍은 검증 키를 반영/저장 → 중복 판정에 재활용
        self._collect_validation_keys()
        try:
            stats = self._compute_import_stats(ctx)
        except Exception as e:
            self.view.log(f"[사전 체크] 통계 산출 실패: {e}")
            return self.view.show_confirm(
                "Import 사전 체크",
                f"사전 체크 중 오류가 발생했습니다:\n{e}\n\n그래도 계속하시겠습니까?"
            )

        if not stats:
            return True  # 요약할 내용이 없으면 그대로 진행

        body = self._format_import_stats(stats, ctx['params']['if_exists'])
        return self.view.show_confirm("Import 사전 체크", f"{body}\n\n계속하시겠습니까?")

    def _compute_import_stats(self, ctx):
        """소스 파일을 읽어 비교 대상 테이블별 사전 통계를 산출한다.
        반환: [{table, file_rows, db_rows, dup_rows, key_columns}]"""
        params = ctx['params']
        mode = ctx['mode']
        db_config = ctx['db_config']

        tables = self._load_source_tables(
            mode, params['file_path'], params['import_scope'], params['source_name'], ctx
        )

        stats = []
        for comp in ctx['comparisons']:
            target = comp['target_table']
            df = tables.get(target)
            if df is None:
                continue

            file_rows = len(df)
            norm_cols = [str(c).strip().replace(" ", "_").lower() for c in df.columns]

            db_rows = fetch_table_row_count(db_config, target)
            dup_rows = None
            key_columns = []
            key_source = None

            if db_rows is not None:
                # 1순위: Test에서 사용자가 찍은 검증 키(가장 정확) → 재활용
                user_keys = self._validation_keys.get(target)
                if user_keys:
                    keys = list(user_keys)
                    key_source = 'user'
                else:
                    # 2순위: PK/UNIQUE 자동 인식 (단, auto_increment 컬럼은 새 값 부여되므로 제외)
                    keys = fetch_key_columns(db_config, target)
                    auto_cols = {
                        cn for cn, _dt, _ck, ex in (comp.get('mysql_columns') or [])
                        if 'auto_increment' in (ex or '').lower()
                    }
                    keys = [k for k in keys if k not in auto_cols]
                    key_source = 'auto' if keys else None

                if keys:
                    norm_key = [k.strip().replace(" ", "_").lower() for k in keys]
                    if all(k in norm_cols for k in norm_key):
                        key_columns = keys
                        col_idx = {c: i for i, c in enumerate(norm_cols)}
                        sub = df.iloc[:, [col_idx[k] for k in norm_key]]
                        key_tuples = self._distinct_key_tuples(sub)
                        dup_rows = (
                            count_existing_keys(db_config, target, keys, key_tuples)
                            if key_tuples else 0
                        )
                    else:
                        # 키가 있으나 파일에 키 컬럼이 없음 → 확인 불가
                        key_columns = keys
                        dup_rows = None

            stats.append({
                'table': target,
                'file_rows': file_rows,
                'db_rows': db_rows,
                'dup_rows': dup_rows,
                'key_columns': key_columns,
                'key_source': key_source,
            })
        return stats

    def _load_source_tables(self, mode, file_path, import_scope, source_name, ctx):
        """소스 파일을 {비교 대상 테이블명: DataFrame} 형태로 읽는다.
        키는 _refresh_comparison_preview / _start_import_comparison 의 target 산출 규칙과 일치시킨다."""
        result = {}
        if mode == "pkl2mysql":
            data = pd.read_pickle(file_path)
            if import_scope == "single":
                target = ctx['params']['target_table']
                if isinstance(data, dict):
                    if source_name and source_name in data:
                        result[target] = data[source_name]
                else:
                    result[target] = data
            else:
                if isinstance(data, dict):
                    for k, v in data.items():
                        if isinstance(v, pd.DataFrame):
                            result[str(k).strip().lower().replace(" ", "_")] = v
                elif isinstance(data, pd.DataFrame):
                    table_name = os.path.basename(file_path).split('.')[0]
                    result[table_name.strip().lower().replace(" ", "_")] = data
        else:  # xlsx2mysql
            if import_scope == "single":
                target = ctx['params']['target_table']
                if source_name:
                    result[target] = pd.read_excel(file_path, sheet_name=source_name)
                else:
                    result[target] = pd.read_excel(file_path, sheet_name=0)
            else:
                sheets = pd.read_excel(file_path, sheet_name=None)
                for sheet_name, df in sheets.items():
                    result[str(sheet_name).strip().lower().replace(" ", "_")] = df
        return result

    @staticmethod
    def _distinct_key_tuples(sub_df):
        """키 컬럼 DataFrame에서 NULL 없는 고유 키 튜플 목록(파이썬 native 값)을 만든다."""
        seen = set()
        result = []
        for row in sub_df.itertuples(index=False, name=None):
            conv = []
            has_null = False
            for v in row:
                if pd.api.types.is_scalar(v) and pd.isna(v):
                    has_null = True
                    break
                conv.append(v.item() if hasattr(v, "item") else v)
            if has_null:
                continue
            t = tuple(conv)
            if t not in seen:
                seen.add(t)
                result.append(t)
        return result

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

    def _build_comparisons(self, db_config, params):
        """params + 캐시를 바탕으로 [{target_table, df_columns, mysql_columns}] 리스트를 만든다.
        파일 미선택('all' 모드에서 캐시 없음) 시 빈 리스트를 반환한다."""
        comparisons = []
        if params['import_scope'] == "single":
            target = params['target_table']
            source_name = params['source_name']
            df_columns = self._find_cached_columns(source_name, target, params['file_path'])
            mysql_columns = fetch_table_columns(db_config, target)
            self._close_tunnel()
            comparisons.append({
                'target_table': target,
                'df_columns': df_columns,
                'mysql_columns': mysql_columns,
            })
        else:
            if not self._cached_source_columns:
                return []
            for source_name, df_cols in self._cached_source_columns.items():
                target = source_name.strip().lower().replace(" ", "_")
                mysql_columns = fetch_table_columns(db_config, target)
                self._close_tunnel()
                comparisons.append({
                    'target_table': target,
                    'df_columns': df_cols,
                    'mysql_columns': mysql_columns,
                })
        return comparisons

    def _start_import_comparison(self, db_url, db_config, params, mode):
        """Build comparison data for all tables and start the wizard."""
        try:
            comparisons = self._build_comparisons(db_config, params)
            if not comparisons:
                if params['import_scope'] != "single" and not self._cached_source_columns:
                    self.view.show_warning("Warning", "파일을 먼저 선택해주세요 (Browse).")
                else:
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

        # --- 사전 체크 (execute 전): 파일 내용·DB 현황·중복 요약 ---
        if not self._import_precheck_confirm(ctx):
            self.view.log("[Import] 사전 체크 단계에서 중단되었습니다.")
            if self._cached_source_columns:
                self._refresh_comparison_preview()
            return

        if self._conn_mgr.is_prod:
            if not self.view.show_confirm(
                "운영환경 Import 확인",
                "운영환경에 대한 Import입니다. 정말 진행하시겠습니까?"
            ):
                self.view.log("[Import] 운영환경 Import가 취소되었습니다.")
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
