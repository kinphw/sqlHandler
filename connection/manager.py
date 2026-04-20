import os
import time

import sqlalchemy

from mysql.services.engine_factory import create_mysql_engine, dispose_mysql_engine


class ConnectionManager:
    """Shared MySQL connection state used by all tabs."""

    def __init__(self):
        # User-facing fields (from UI or .env)
        self.host = ""
        self.port = 3306
        self.user = ""
        self.password = ""
        self.db_name = ""
        self.is_prod = False

        # Internal — effective values after SSH setup
        self._engine = None
        self._tunnel = None
        self._eff_host = ""
        self._eff_port = 3306

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------
    def load_env_defaults(self, use_prod: bool = False):
        prefix = "PROD_" if use_prod else ""
        self.host = os.getenv(f"{prefix}MYSQL_HOST", "")
        port_str = os.getenv(f"{prefix}MYSQL_PORT", "3306")
        self.port = int(port_str) if port_str.isdigit() else 3306
        self.user = os.getenv(f"{prefix}MYSQL_USER", "")
        self.password = os.getenv(f"{prefix}MYSQL_PASSWORD", "")
        self.db_name = os.getenv(f"{prefix}MYSQL_DB", "")
        self.is_prod = use_prod

    def is_configured(self) -> bool:
        return all([self.host, self.user, self.password, self.db_name])

    def is_connected(self) -> bool:
        return self._engine is not None

    # ------------------------------------------------------------------
    # Effective URL / config (post-tunnel)
    # ------------------------------------------------------------------
    def get_db_url(self) -> str:
        return self._build_url(self._eff_host or self.host, self._eff_port or self.port)

    def get_config(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "host": self._eff_host or self.host,
            "port": self._eff_port or self.port,
            "database": self.db_name,
        }

    def get_engine(self):
        return self._engine

    def _build_url(self, host: str, port: int) -> str:
        return (
            f"mysql+pymysql://{self.user}:{self.password}"
            f"@{host}:{port}/{self.db_name}?charset=utf8mb4"
        )

    # ------------------------------------------------------------------
    # Connect / Release
    # ------------------------------------------------------------------
    def connect(self, on_error=None) -> bool:
        """Create engine (+ SSH tunnel if prod+SSH configured). Returns True on success."""
        self.release()

        if not self.is_configured():
            if on_error:
                on_error("연결 정보가 완전하지 않습니다. (Host / User / Password / DB 필수)")
            return False

        eff_host, eff_port = self.host, self.port

        # SSH tunnel (prod only)
        if self.is_prod and os.getenv("SSH_HOST"):
            try:
                import paramiko
                if not hasattr(paramiko, "DSSKey"):
                    class DSSKey:
                        pass
                    paramiko.DSSKey = DSSKey

                from sshtunnel import SSHTunnelForwarder

                ssh_bind_port = int(os.getenv("SSH_BIND_PORT", 13306))
                tunnel = SSHTunnelForwarder(
                    (os.getenv("SSH_HOST"), 22),
                    ssh_username=os.getenv("SSH_USER"),
                    ssh_password=os.getenv("SSH_PASSWORD"),
                    remote_bind_address=("127.0.0.1", self.port),
                    local_bind_address=("127.0.0.1", ssh_bind_port),
                    set_keepalive=10.0,
                )
                tunnel.start()
                time.sleep(1.0)
                self._tunnel = tunnel
                eff_host, eff_port = "127.0.0.1", ssh_bind_port

            except ImportError:
                if on_error:
                    on_error("sshtunnel 모듈 없음 — SSH 없이 직접 연결을 시도합니다.")
            except Exception as e:
                if on_error:
                    on_error(f"SSH 터널 생성 실패: {e}")
                return False

        try:
            url = self._build_url(eff_host, eff_port)
            engine = create_mysql_engine(url)
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))

            self._engine = engine
            self._eff_host = eff_host
            self._eff_port = eff_port
            return True

        except Exception as e:
            if on_error:
                on_error(f"DB 연결 실패: {e}")
            if self._tunnel:
                try:
                    self._tunnel.stop()
                except Exception:
                    pass
                self._tunnel = None
            return False

    def release(self):
        dispose_mysql_engine(self._engine)
        self._engine = None
        self._eff_host = ""
        self._eff_port = 3306
        if self._tunnel:
            try:
                self._tunnel.stop()
            except Exception:
                pass
            self._tunnel = None
