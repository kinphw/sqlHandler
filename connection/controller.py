import os
import threading


class ConnectionController:
    def __init__(self, view, connection_manager):
        self._view = view
        self._conn_mgr = connection_manager

        view.bind_event("load_dev",  lambda: self._load_env(False))
        view.bind_event("load_prod", lambda: self._load_env(True))
        view.bind_event("connect",   self._on_connect)
        view.bind_event("release",   self._on_release)

        # Auto-load dev defaults on startup
        self._load_env(False)

        # Show SSH hint if configured
        if os.getenv("SSH_HOST"):
            self._view.set_ssh_label(
                f"SSH: {os.getenv('SSH_HOST')} — Prod 기본값 선택 시 자동으로 터널을 사용합니다."
            )

    # ------------------------------------------------------------------
    # Load .env defaults
    # ------------------------------------------------------------------
    def _load_env(self, use_prod: bool):
        self._conn_mgr.load_env_defaults(use_prod)
        m = self._conn_mgr
        self._view.set_values(
            host=m.host, port=str(m.port),
            user=m.user, password=m.password,
            db_name=m.db_name,
        )
        label = "Prod" if use_prod else "Dev"
        self._view.log(f"[설정] .env에서 {label} 기본값을 불러왔습니다.")

    # ------------------------------------------------------------------
    # Connect
    # ------------------------------------------------------------------
    def _on_connect(self):
        vals = self._view.get_values()
        try:
            port = int(vals["port"]) if vals["port"] else 3306
        except ValueError:
            self._view.log("[오류] 포트는 숫자여야 합니다.")
            return

        self._conn_mgr.host = vals["host"]
        self._conn_mgr.port = port
        self._conn_mgr.user = vals["user"]
        self._conn_mgr.password = vals["password"]
        self._conn_mgr.db_name = vals["db_name"]

        env_label = "Prod" if self._conn_mgr.is_prod else "Dev"
        self._view.log(
            f"\n[연결 시도] ({env_label}) "
            f"{vals['user']}@{vals['host']}:{port}/{vals['db_name']}"
        )
        self._view.set_status("연결 중...", "orange")

        threading.Thread(target=self._do_connect, daemon=True).start()

    def _do_connect(self):
        success = self._conn_mgr.connect(
            on_error=lambda msg: self._view.log(f"[오류] {msg}")
        )

        def _update():
            if success:
                m = self._conn_mgr
                self._view.set_status(
                    f"● 연결됨: {m.user}@{m._eff_host}:{m._eff_port}/{m.db_name}",
                    "#2d8a2d",
                )
                self._view.log("[연결 성공]")
            else:
                self._view.set_status("✕ 연결 실패", "red")
                self._view.log("[연결 실패]")

        self._view.schedule(_update)

    # ------------------------------------------------------------------
    # Release
    # ------------------------------------------------------------------
    def _on_release(self):
        self._conn_mgr.release()
        self._view.set_status("연결 없음", "gray")
        self._view.log("[연결 해제] 엔진 및 터널 해제 완료")
