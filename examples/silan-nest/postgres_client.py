import uuid
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple
import psycopg
from psycopg.rows import dict_row
class PostgresClient:
    def __init__(self, dsn: Optional[str] = None, **connect_kwargs: Any) -> None:
        if not dsn and not connect_kwargs:
            raise ValueError("Provide either a DSN or explicit connection keyword arguments.")
        self._dsn = dsn
        self._connect_kwargs = connect_kwargs

    @contextmanager
    def connection(self):
        if self._dsn:
            conn = psycopg.connect(self._dsn, row_factory=dict_row)
        else:
            conn = psycopg.connect(row_factory=dict_row, **self._connect_kwargs)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()