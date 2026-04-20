import threading
from tkinter import messagebox

import sqlalchemy


class CleanerController:
    def __init__(self, view, connection_manager):
        self._view = view
        self._conn_mgr = connection_manager
        self._id_col = None
        self._columns = []
        self._last_query = None
        self._last_page_max_id = None
        self._current_page_first_id = None
        self._current_page_last_id = None

        view.bind_event("query",      self._on_query)
        view.bind_event("next_page",  self._on_next_page)
        view.bind_event("delete_key", self._on_delete_key)

        self._refresh_conn_label()

    def _refresh_conn_label(self):
        if self._conn_mgr.is_connected():
            m = self._conn_mgr
            self._view.set_conn_label(
                f"연결됨: {m.user}@{m._eff_host}:{m._eff_port}/{m.db_name}",
                "#2d8a2d",
            )
        else:
            self._view.set_conn_label("연결 없음 — 'DB 연결' 탭에서 연결하세요", "gray")

    # ------------------------------------------------------------------
    # Query — params read on main thread, log via after()
    # ------------------------------------------------------------------
    def _on_query(self):
        self._refresh_conn_label()

        if not self._conn_mgr.is_connected():
            messagebox.showwarning("연결 없음", "'DB 연결' 탭에서 먼저 DB에 연결하세요.")
            return

        table = self._view.get_table_name()
        if not table:
            messagebox.showwarning("입력 오류", "테이블 이름을 입력하세요.")
            return

        # Read everything on the main thread before spawning
        where = self._view.get_where_clause()
        start_id = self._view.get_start_id()
        end_id = self._view.get_end_id()
        limit = self._view.get_limit()
        db_name = self._conn_mgr.db_name

        msg = f"\n[조회] {db_name}.{table}"
        if where:
            msg += f" WHERE {where}"
        if start_id or end_id:
            msg += f" | ID 범위: {start_id or '-'} ~ {end_id or '-'}"
        msg += f" LIMIT {limit}"
        self._view.log(msg)
        self._view.set_next_enabled(False)

        threading.Thread(
            target=self._do_query,
            args=(table, where, limit, start_id, end_id, None),
            daemon=True,
        ).start()

    def _on_next_page(self):
        self._refresh_conn_label()

        if not self._conn_mgr.is_connected():
            messagebox.showwarning("연결 없음", "'DB 연결' 탭에서 먼저 DB에 연결하세요.")
            return

        if not self._last_query:
            messagebox.showwarning("조회 필요", "먼저 첫 조회를 실행하세요.")
            return

        if not self._id_col:
            messagebox.showwarning("키 컬럼 없음", "키 컬럼이 감지된 테이블만 다음 묶음 조회를 지원합니다.")
            return

        if self._last_page_max_id is None:
            messagebox.showinfo("마지막 페이지", "더 이상 조회할 데이터가 없습니다.")
            return

        last_query = self._last_query
        self._view.log(
            f"\n[다음 조회] {last_query['table']} | {self._id_col} > {self._last_page_max_id} LIMIT {last_query['limit']}"
        )
        self._view.set_next_enabled(False)

        threading.Thread(
            target=self._do_query,
            args=(
                last_query["table"],
                last_query["where"],
                last_query["limit"],
                last_query["start_id"],
                last_query["end_id"],
                self._last_page_max_id,
            ),
            daemon=True,
        ).start()

    def _do_query(
        self,
        table: str,
        where: str,
        limit: int,
        start_id: str,
        end_id: str,
        after_id,
    ):
        # All UI writes go through after() — never call tkinter directly here
        schedule = self._view.schedule

        engine = self._conn_mgr.get_engine()
        try:
            with engine.connect() as conn:
                meta = conn.execute(sqlalchemy.text(f"SELECT * FROM `{table}` LIMIT 0"))
                cols = list(meta.keys())
                id_col = self._detect_id_col(cols)

                if (start_id or end_id or after_id is not None) and not id_col:
                    raise ValueError("키 컬럼이 감지되지 않아 ID 범위를 사용할 수 없습니다.")

                sql, params = self._build_query(
                    table=table,
                    where=where,
                    limit=limit,
                    id_col=id_col,
                    start_id=start_id,
                    end_id=end_id,
                    after_id=after_id,
                )
                schedule(lambda m=f"  SQL → {sql}": self._view.log(m))

                result = conn.execute(sqlalchemy.text(sql), params)
                cols = list(result.keys())
                rows = [tuple(row) for row in result.fetchall()]

            self._columns = cols
            self._id_col = id_col
            id_col = self._id_col
            self._last_query = {
                "table": table,
                "where": where,
                "limit": limit,
                "start_id": start_id,
                "end_id": end_id,
            }
            self._current_page_first_id, self._current_page_last_id = self._get_page_id_bounds(
                rows, cols, id_col
            )
            self._last_page_max_id = self._current_page_last_id

            msg = f"  → {len(rows):,}건 조회 완료 | 키 컬럼: {id_col or '감지 실패'}"
            if rows and self._last_page_max_id is not None and id_col:
                msg += f" | 현재 범위: {self._current_page_first_id} ~ {self._last_page_max_id}"
            schedule(lambda m=msg: self._view.log(m))
            schedule(lambda: self._apply_results(cols, rows))

        except Exception as e:
            self._last_page_max_id = None
            self._current_page_first_id = None
            self._current_page_last_id = None
            schedule(lambda: self._view.set_next_enabled(False))
            schedule(lambda m=f"[오류] 조회 실패: {e}": self._view.log(m))

    def _apply_results(self, cols, rows):
        self._view.set_columns(cols)
        self._view.set_id_col_label(self._id_col or "없음")
        self._view.start_populate(rows)
        self._update_page_status(len(rows))
        self._view.set_next_enabled(bool(rows) and bool(self._id_col))

    @staticmethod
    def _build_query(table, where, limit, id_col, start_id="", end_id="", after_id=None):
        conditions = []
        params = {"limit": limit}

        if where:
            conditions.append(f"({where})")
        if start_id:
            conditions.append(f"`{id_col}` >= :start_id")
            params["start_id"] = start_id
        if end_id:
            conditions.append(f"`{id_col}` <= :end_id")
            params["end_id"] = end_id
        if after_id is not None:
            conditions.append(f"`{id_col}` > :after_id")
            params["after_id"] = after_id

        sql = f"SELECT * FROM `{table}`"
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        if id_col:
            sql += f" ORDER BY `{id_col}` ASC"
        sql += " LIMIT :limit"
        return sql, params

    @staticmethod
    def _get_page_id_bounds(rows, cols, id_col):
        if not rows or not id_col or id_col not in cols:
            return None, None
        id_idx = cols.index(id_col)
        return rows[0][id_idx], rows[-1][id_idx]

    def _update_page_status(self, count=None):
        rows = self._view.get_all_row_values()
        if count is None:
            count = len(rows)

        if self._id_col and self._columns and rows:
            first_id, last_id = self._get_page_id_bounds(rows, self._columns, self._id_col)
        else:
            first_id, last_id = None, None

        self._current_page_first_id = first_id
        self._current_page_last_id = last_id
        self._view.set_count_label(
            count,
            id_col=self._id_col,
            first_id=first_id,
            last_id=last_id,
        )

    @staticmethod
    def _detect_id_col(cols: list):
        lower = [c.lower() for c in cols]
        if "id" in lower:
            return cols[lower.index("id")]
        return cols[0] if cols else None

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------
    def _on_delete_key(self, event=None):
        selected = self._view.get_selected_iids()
        if not selected:
            return

        if not self._conn_mgr.is_connected():
            messagebox.showwarning("연결 없음", "'DB 연결' 탭에서 먼저 DB에 연결하세요.")
            return

        if not self._id_col:
            messagebox.showwarning("오류", "키 컬럼이 감지되지 않아 삭제할 수 없습니다.")
            return

        id_idx = self._columns.index(self._id_col)
        ids = [self._view.get_row_values(iid)[id_idx] for iid in selected]
        table = self._view.get_table_name()
        db_name = self._conn_mgr.db_name

        preview = str(ids[:10]) + ("..." if len(ids) > 10 else "")
        confirmed = messagebox.askyesno(
            "삭제 확인",
            f"[{db_name}.{table}] 에서 {len(ids)}건을 삭제합니다.\n\n"
            f"키 컬럼 : {self._id_col}\n"
            f"대상 ID : {preview}\n\n"
            f"계속하시겠습니까?"
        )
        if not confirmed:
            self._view.log("[삭제 취소]")
            return

        threading.Thread(
            target=self._do_delete, args=(table, ids, selected), daemon=True
        ).start()

    def _do_delete(self, table: str, ids: list, iids: tuple):
        schedule = self._view.schedule
        schedule(lambda m=f"\n[삭제 요청] {table}.{self._id_col} IN {ids}": self._view.log(m))

        try:
            placeholders = ", ".join(f":id_{i}" for i in range(len(ids)))
            sql = f"DELETE FROM `{table}` WHERE `{self._id_col}` IN ({placeholders})"
            params = {f"id_{i}": v for i, v in enumerate(ids)}

            schedule(lambda m=f"  SQL → {sql}": self._view.log(m))

            engine = self._conn_mgr.get_engine()
            with engine.begin() as conn:
                result = conn.execute(sqlalchemy.text(sql), params)
                affected = result.rowcount

            schedule(lambda m=f"  → 삭제 완료: {affected}건": self._view.log(m))
            schedule(lambda: self._remove_and_update(iids))

        except Exception as e:
            schedule(lambda m=f"[오류] 삭제 실패: {e}": self._view.log(m))

    def _remove_and_update(self, iids: tuple):
        self._view.remove_rows(iids)
        self._update_page_status()
