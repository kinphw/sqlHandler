from sqlalchemy import create_engine


def create_mysql_engine(db_url):
    return create_engine(db_url)


def dispose_mysql_engine(engine, logger=None, label=None):
    if not engine:
        return

    engine.dispose()
    if logger and label:
        logger(f"🔒 [{label}] 데이터베이스 연결 해제")
