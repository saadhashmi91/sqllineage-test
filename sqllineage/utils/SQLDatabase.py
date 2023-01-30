"""SQLAlchemy wrapper around a database."""
from __future__ import annotations

from typing import Any, Iterable, List, Optional

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine


class SQLDatabase:
    """SQLAlchemy wrapper around a database."""

    def __init__(
        self,
        engine: Engine,
        schema: Optional[str] = None,
        ignore_tables: Optional[List[str]] = None,
        include_tables: Optional[List[str]] = None,
        sample_row_in_table_info: bool = False,
    ):
        """Create engine from database URI."""
        self._engine = engine
        self._schema = schema
        if include_tables and ignore_tables:
            raise ValueError("Cannot specify both include_tables and ignore_tables")

        self._inspector = inspect(self._engine)
        self._all_tables = self._inspector.get_table_names(schema=schema)
        self._include_tables = include_tables or []
        if self._include_tables:
            missing_tables = set(self._include_tables).difference(self._all_tables)
            if missing_tables:
                raise ValueError(
                    f"include_tables {missing_tables} not found in database"
                )
        self._ignore_tables = ignore_tables or []
        if self._ignore_tables:
            missing_tables = set(self._ignore_tables).difference(self._all_tables)
            if missing_tables:
                raise ValueError(
                    f"ignore_tables {missing_tables} not found in database"
                )
        self._sample_row_in_table_info = sample_row_in_table_info

    @classmethod
    def from_uri(cls, database_uri: str, **kwargs: Any) -> SQLDatabase:
        """Construct a SQLAlchemy engine from URI."""
        return cls(create_engine(database_uri), **kwargs)

    @property
    def dialect(self) -> str:
        """Return string representation of dialect to use."""
        return self._engine.dialect.name

    def get_table_names(self) -> Iterable[str]:
        """Get names of tables available."""
        if self._include_tables:
            return self._include_tables
        return set(self._all_tables) - set(self._ignore_tables)

    @property
    def table_info(self) -> str:
        """Information about all tables in the database."""
        return self.get_table_info()

    def get_table_info(self, table_names: Optional[List[str]] = None) -> str:
        """Get information about specified tables."""
        all_table_names = self.get_table_names()
        if table_names is not None:
            missing_tables = set(table_names).difference(all_table_names)
            if missing_tables:
                raise ValueError(f"table_names {missing_tables} not found in database")
            all_table_names = table_names

        template = "Table '{table_name}' has columns: {columns}."

        tables = {}
        for table_name in all_table_names:

            columns = []
            for column in self._inspector.get_columns(table_name, schema=self._schema):
                columns.append(f"{column['name']}")
            tables[table_name] = columns
        return tables