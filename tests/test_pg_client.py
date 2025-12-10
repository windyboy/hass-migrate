import pytest

from hass_migrate.database.pg_client import PGClient


def test_split_sql_statements_basic() -> None:
    sql = """
        -- set timezone
        SET timezone = 'UTC';

        /* create a table */
        CREATE TABLE example (
            id SERIAL PRIMARY KEY,
            name TEXT
        );

        CREATE INDEX idx_example_name ON example(name);
    """

    statements = PGClient._split_sql_statements(sql)

    assert statements == [
        "SET timezone = 'UTC'",
        "CREATE TABLE example (\n            id SERIAL PRIMARY KEY,\n            name TEXT\n        )",
        "CREATE INDEX idx_example_name ON example(name)",
    ]


def test_split_sql_statements_ignores_semicolons_in_single_quotes() -> None:
    sql = """
        INSERT INTO logs(message) VALUES('value; still message');
        INSERT INTO logs(message) VALUES('final');
    """

    statements = PGClient._split_sql_statements(sql)

    assert statements == [
        "INSERT INTO logs(message) VALUES('value; still message')",
        "INSERT INTO logs(message) VALUES('final')",
    ]


def test_split_sql_statements_ignores_semicolons_in_double_quotes() -> None:
    sql = """
        CREATE TABLE "semi;colon" (
            id SERIAL PRIMARY KEY
        );
        COMMENT ON TABLE "semi;colon" IS 'metadata';
    """

    statements = PGClient._split_sql_statements(sql)

    assert statements == [
        'CREATE TABLE "semi;colon" (\n            id SERIAL PRIMARY KEY\n        )',
        "COMMENT ON TABLE \"semi;colon\" IS 'metadata'",
    ]


def test_split_sql_statements_handles_dollar_quoted_functions() -> None:
    sql = """
        CREATE FUNCTION increment_counter()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$BEGIN
            NEW.counter := NEW.counter + 1;
            RETURN NEW;
        END;$$;

        CREATE TRIGGER update_counter
        BEFORE INSERT ON metrics
        FOR EACH ROW
        EXECUTE FUNCTION increment_counter();
    """

    statements = PGClient._split_sql_statements(sql)

    assert statements == [
        "CREATE FUNCTION increment_counter()\n        RETURNS TRIGGER\n        LANGUAGE plpgsql\n        AS $$BEGIN\n            NEW.counter := NEW.counter + 1;\n            RETURN NEW;\n        END;$$",
        "CREATE TRIGGER update_counter\n        BEFORE INSERT ON metrics\n        FOR EACH ROW\n        EXECUTE FUNCTION increment_counter()",
    ]


def test_split_sql_statements_trims_trailing_whitespace() -> None:
    sql = "CREATE TABLE example(id INT);\n\n   "

    statements = PGClient._split_sql_statements(sql)

    assert statements == ["CREATE TABLE example(id INT)"]


@pytest.mark.asyncio
async def test_apply_schema_executes_statements(tmp_path) -> None:
    sql_path = tmp_path / "schema.sql"
    sql_path.write_text("SET timezone = 'UTC';\nCREATE TABLE sample(id INT);\n")

    class DummyTransaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummyConnection:
        def __init__(self) -> None:
            self.executed: list[str] = []

        async def execute(self, statement: str) -> None:
            self.executed.append(statement)

        def transaction(self):
            return DummyTransaction()

    class DummyAcquire:
        def __init__(self, connection: DummyConnection) -> None:
            self._connection = connection

        async def __aenter__(self) -> DummyConnection:
            return self._connection

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

    class DummyPool:
        def __init__(self, connection: DummyConnection) -> None:
            self._connection = connection

        def acquire(self) -> DummyAcquire:
            return DummyAcquire(self._connection)

    connection = DummyConnection()
    client = object.__new__(PGClient)
    client.config = None
    client.schema = "public"
    client.pool = DummyPool(connection)

    await client.apply_schema(str(sql_path))

    assert connection.executed == [
        "SET timezone = 'UTC'",
        "CREATE TABLE sample(id INT)",
    ]
