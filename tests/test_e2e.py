import asyncio
import os

import asyncpg
import docker
import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from hass_migrate.config import DBConfig
from hass_migrate.database.mysql_client import MySQLClient
from hass_migrate.database.pg_client import PGClient
from hass_migrate.models.table_metadata import MigrationConfig
from hass_migrate.services.migration_service import MigrationService
from hass_migrate.utils.dependency import DependencyAnalyzer
from hass_migrate.utils.logger import StructuredLogger


def docker_available():
    try:
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


# Define path to schema files
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MYSQL_SCHEMA_PATH = os.path.join(PROJECT_ROOT, "hass_migrate", "schema", "schema.sql")
PG_SCHEMA_PATH = os.path.join(
    PROJECT_ROOT, "hass_migrate", "schema", "postgres_schema.sql"
)


@pytest.fixture(scope="module")
def mysql_container():
    if not docker_available():
        pytest.skip("Docker not available")
    try:
        with MySqlContainer("mariadb:10.11") as mysql:
            yield mysql
    except Exception:
        pytest.skip("Docker container setup failed")


@pytest.fixture(scope="module")
def postgres_container():
    if not docker_available():
        pytest.skip("Docker not available")
    try:
        with PostgresContainer("postgres:15") as postgres:
            yield postgres
    except Exception:
        pytest.skip("Docker container setup failed")


@pytest.fixture(scope="module")
def db_config(mysql_container, postgres_container):
    """Setup environment variables for DBConfig."""
    # MySQL
    os.environ["MYSQL_HOST"] = mysql_container.get_container_host_ip()
    os.environ["MYSQL_PORT"] = str(mysql_container.get_exposed_port(3306))
    os.environ["MYSQL_USER"] = mysql_container.username
    os.environ["MYSQL_PASSWORD"] = mysql_container.password
    os.environ["MYSQL_DB"] = mysql_container.dbname

    # Postgres
    os.environ["PG_HOST"] = postgres_container.get_container_host_ip()
    os.environ["PG_PORT"] = str(postgres_container.get_exposed_port(5432))
    os.environ["PG_USER"] = postgres_container.username
    os.environ["PG_PASSWORD"] = postgres_container.password
    os.environ["PG_DB"] = postgres_container.dbname
    os.environ["PG_SCHEMA"] = "hass"

    return DBConfig()


def execute_sql_file(engine, file_path):
    with open(file_path, "r") as f:
        content = f.read()
        # Split by ; and execute each statement
        # This is a naive split, but might work for simple dumps
        # Better to use a proper runner or just execute the whole thing if the driver supports it
        with engine.connect() as conn:
            # MariaDB dump has comments and specific syntax.
            # SQLAlchemy execute might fail on some specific commands if not handled.
            # For simplicity, let's try to execute the whole block if possible,
            # or split by statement.
            # The dump has /*! ... */ comments which are executable in MySQL.

            # Using raw connection for multi-statement might be better
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            # Read the file and execute as script
            # mysql-connector-python cursor has execute(multi=True)
            # But sqlalchemy connection might be different.
            pass

    # Re-implementing simple execution
    # We will use the container's get_connection_url to create an engine
    pass


def setup_mysql_data(mysql_container):
    url = mysql_container.get_connection_url()
    # Use PyMySQL
    if "mysql+pymysql" not in url:
        if url.startswith("mysql://"):
            url = url.replace("mysql://", "mysql+pymysql://")
        elif url.startswith("mysql+mysqldb://"):
            url = url.replace("mysql+mysqldb://", "mysql+pymysql://")

    engine = sqlalchemy.create_engine(url)

    # Read schema.sql
    with open(MYSQL_SCHEMA_PATH, "r") as f:
        sql_script = f.read()

    # Execute schema
    # We split by semicolon, but we need to be careful about semicolons in strings.
    # For this dump, it seems standard.
    statements = sql_script.split(";")
    with engine.connect() as conn:
        for statement in statements:
            stmt = statement.strip()
            if not stmt:
                continue
            # Skip comments
            if stmt.startswith("/*") or stmt.startswith("--"):
                continue

            try:
                conn.execute(text(stmt))
            except Exception as e:
                # print(f"Skipping statement: {stmt[:50]}... Error: {e}")
                pass
        conn.commit()

    # Insert dummy data
    with engine.connect() as conn:
        # event_types
        # Check if table exists first to avoid errors if schema loading failed
        try:
            conn.execute(
                text("INSERT INTO event_types (event_type) VALUES ('state_changed')")
            )
            conn.execute(
                text("INSERT INTO event_types (event_type) VALUES ('call_service')")
            )
            conn.commit()
        except Exception as e:
            pytest.fail(f"Failed to insert dummy data: {e}")


@pytest.mark.skipif(not docker_available(), reason="Docker not available")
@pytest.mark.asyncio
async def test_e2e_migration(mysql_container, postgres_container, db_config):
    # 1. Setup MySQL Data
    setup_mysql_data(mysql_container)

    # 2. Setup Postgres Schema
    pg_engine = sqlalchemy.create_engine(postgres_container.get_connection_url())

    # Create schema
    with pg_engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {db_config.pg_schema}"))
        conn.commit()

    # Apply postgres_schema.sql
    with open(PG_SCHEMA_PATH, "r") as f:
        pg_sql = f.read()

    with pg_engine.connect() as conn:
        conn.execute(text(f"SET search_path TO {db_config.pg_schema}"))
        try:
            conn.execute(text(pg_sql))
            conn.commit()
        except Exception as e:
            print(f"Error applying PG schema: {e}")
            pass

    # 3. Test direct client operations
    mysql_client = MySQLClient(db_config)
    await mysql_client.connect()

    pg_client = PGClient(db_config, schema=db_config.pg_schema)
    await pg_client.connect()

    # Fetch data from MySQL
    rows = await mysql_client.fetch_batch(
        "event_types", ["event_type_id", "event_type"], 1000
    )
    assert len(rows) == 2

    # Insert into PostgreSQL
    await pg_client.batch_insert_executemany(
        "event_types", ["event_type_id", "event_type"], rows, schema=db_config.pg_schema
    )

    # Verify data in Postgres
    count = await pg_client.count_rows("event_types", schema=db_config.pg_schema)
    assert count == 2

    await pg_client.close()
    await mysql_client.close()
