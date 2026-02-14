import os
import time
from typing import Callable, Optional, Tuple


def get_db_url_and_config(
    db_name: str,
    use_prod: bool,
    env_getter: Callable[[str, Optional[str]], Optional[str]] = os.getenv,
    silent: bool = False,
    on_error: Optional[Callable[[str, str], None]] = None,
) -> Tuple[Optional[str], Optional[dict], Optional[object]]:
    """Return (db_url, config, tunnel)."""
    if not db_name:
        if not silent and on_error:
            on_error("Error", "Database name is required.")
        return None, None, None

    prefix = "PROD_" if use_prod else ""

    config = {
        'user': env_getter(f"{prefix}MYSQL_USER", None),
        'password': env_getter(f"{prefix}MYSQL_PASSWORD", None),
        'host': env_getter(f"{prefix}MYSQL_HOST", None),
        'port': int(env_getter(f"{prefix}MYSQL_PORT", 3306)),
        'database': db_name
    }

    if not all(config.values()):
        if not silent and on_error:
            on_error("Configuration Error", "Missing .env configuration for selected environment.")
        return None, None, None

    tunnel = None
    if use_prod and env_getter("SSH_HOST", None):
        try:
            import paramiko
            if not hasattr(paramiko, 'DSSKey'):
                class DSSKey:
                    pass
                paramiko.DSSKey = DSSKey

            from sshtunnel import SSHTunnelForwarder

            ssh_host = env_getter("SSH_HOST", None)
            ssh_user = env_getter("SSH_USER", None)
            ssh_password = env_getter("SSH_PASSWORD", None)
            ssh_bind_port = int(env_getter("SSH_BIND_PORT", 13306))

            tunnel = SSHTunnelForwarder(
                (ssh_host, 22),
                ssh_username=ssh_user,
                ssh_password=ssh_password,
                remote_bind_address=('127.0.0.1', config['port']),
                local_bind_address=('127.0.0.1', ssh_bind_port),
                set_keepalive=10.0
            )
            tunnel.start()
            time.sleep(1.0)

            config['host'] = '127.0.0.1'
            config['port'] = ssh_bind_port
        except ImportError:
            print("⚠️ sshtunnel module not found. Skipping SSH tunnel.")
        except Exception as e:
            if not silent and on_error:
                on_error("SSH Error", f"Failed to create SSH tunnel: {e}")
            return None, None, None

    db_url = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}?charset=utf8mb4"
    )
    return db_url, config, tunnel
