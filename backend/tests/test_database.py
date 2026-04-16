from __future__ import annotations

from pathlib import Path

from app.config import Settings
from app.database import Database


class _FakeCursor:
    def fetchone(self):  # noqa: ANN201
        return None

    def fetchall(self):  # noqa: ANN201
        return []


class _FakePostgresConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[int, ...]]] = []
        self.committed = False
        self.closed = False

    def __enter__(self) -> "_FakePostgresConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        self.closed = True

    def execute(self, query: str, params: tuple[int, ...] = ()) -> _FakeCursor:
        self.calls.append((query, params))
        return _FakeCursor()

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        raise AssertionError("rollback should not be called in the success path")


def test_settings_database_target_prefers_database_url(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        database_url="postgresql://portfolio:secret@localhost:5432/portfolio",
        sqlite_path_override=tmp_path / "local.db",
    )

    assert settings.database_target == "postgresql://portfolio:secret@localhost:5432/portfolio"


def test_settings_database_target_falls_back_to_sqlite(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        sqlite_path_override=tmp_path / "local.db",
    )

    assert settings.database_target == tmp_path / "local.db"


def test_postgres_connections_translate_qmark_placeholders(monkeypatch) -> None:
    database = Database("postgresql://portfolio:secret@localhost:5432/portfolio")
    fake_connection = _FakePostgresConnection()
    monkeypatch.setattr(database, "_connect_postgres", lambda: fake_connection)

    with database.connect() as connection:
        connection.execute("SELECT ? AS first_value, ? AS second_value", (1, 2))

    assert fake_connection.calls == [
        ("SELECT %s AS first_value, %s AS second_value", (1, 2)),
    ]
    assert fake_connection.committed is True
    assert fake_connection.closed is True


def test_sqlite_url_is_parsed_into_path() -> None:
    database = Database("sqlite:///backend/data/test.db")

    assert database.is_postgres is False
    assert database.path == Path("backend/data/test.db")
