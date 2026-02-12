from contextlib import contextmanager

from sqlalchemy.orm import sessionmaker

from .engine import engine

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
