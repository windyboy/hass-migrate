import asyncio
import os
import pytest
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer
import sqlalchemy
from sqlalchemy import text
import asyncpg

from hass_migrate.config import DBConfig
from hass_migrate.database.mysql_client import MySQLClient
from hass_migrate.database.pg_client import PGClient
from hass_migrate.services.migration_service import MigrationService
from hass_migrate.utils.dependency import DependencyAnalyzer
from hass_migrate.utils.logger import StructuredLogger
from hass_migrate.models.table_metadata import MigrationConfig

# Define path to schema files
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MYSQL_SCHEMA_PATH = os.path.join(PROJECT_ROOT, "hass_migrate", "schema", "schema.sql")
PG_SCHEMA_PATH = os.path.join(PROJECT_ROOT, "hass_migrate", "schema", "postgres_schema.sql")

@pytest.fixture(scope="module")
def mysql_container():
    with MySqlContainer("mariadb:10.11") as mysql:
        yield mysql

@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:15") as postgres:
        yield postgres

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
    # Force mysql-connector-python
    if "mysql+mysqlconnector" not in url:
        if url.startswith("mysql://"):
            url = url.replace("mysql://", "mysql+mysqlconnector://")
        elif url.startswith("mysql+mysqldb://"):
            url = url.replace("mysql+mysqldb://", "mysql+mysqlconnector://")
            
    engine = sqlalchemy.create_engine(url)
    
    # Read schema.sql
    with open(MYSQL_SCHEMA_PATH, "r") as f:
        sql_script = f.read()
    
    # Execute schema
    # We split by semicolon, but we need to be careful about semicolons in strings.
    # For this dump, it seems standard.
    statements = sql_script.split(';')
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
            conn.execute(text("INSERT INTO event_types (event_type) VALUES ('state_changed')"))
            conn.execute(text("INSERT INTO event_types (event_type) VALUES ('call_service')"))
            conn.commit()
        except Exception as e:
            pytest.fail(f"Failed to insert dummy data: {e}")

@pytest.mark.asyncio
async def test_e2e_migration(mysql_container, postgres_container, db_config):
    # 1. Setup MySQL Data
    setup_mysql_data(mysql_container)
    
    # 2. Setup Postgres Schema
    # We can use the CLI function apply_schema, but we need to mock Typer context or just call the logic.
    # apply_schema logic is in hass_migrate.cli.schema.
    # Let's look at hass_migrate/cli/schema.py to see if we can reuse logic.
    # Or just execute the SQL file directly.
    
    pg_engine = sqlalchemy.create_engine(postgres_container.get_connection_url())
    
    # Create schema
    with pg_engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {db_config.pg_schema}"))
        conn.commit()
        
    # Apply postgres_schema.sql
    with open(PG_SCHEMA_PATH, "r") as f:
        pg_sql = f.read()
        
    # Replace schema placeholder if any, or set search_path
    with pg_engine.connect() as conn:
        conn.execute(text(f"SET search_path TO {db_config.pg_schema}"))
        # Execute statements
        # Postgres schema file usually contains standard SQL.
        # We can try executing it.
        # Note: The file might contain BEGIN/COMMIT.
        try:
            conn.execute(text(pg_sql))
            conn.commit()
        except Exception as e:
            print(f"Error applying PG schema: {e}")
            # If it fails, we might need to split it
            pass

    # 3. Run Migration
    mysql_client = MySQLClient(db_config)
    mysql_client.connect()
    
    pg_client = PGClient(db_config)
    await pg_client.connect()
    
    logger = StructuredLogger("test_migration")
    dep_analyzer = DependencyAnalyzer()
    
    service = MigrationService(mysql_client, pg_client, dep_analyzer, logger)
    
    # Define tables to migrate
    # We need to know which tables exist in MySQL.
    # For this test, let's assume we migrated 'event_types' and 'events' (if we inserted data).
    # Let's migrate all tables that we populated.
    
    # We need to get the list of tables and columns.
    # In the real CLI, this comes from constants.TABLES or inspection.
    from hass_migrate.cli.constants import TABLES
    
    # We only populated event_types, so let's just migrate that for now to prove it works.
    # Or better, populate more data.
    
    # Let's try to migrate 'event_types'
    # We need to find columns for event_types
    # In constants.TABLES, it is a list of table names.
    # The columns are fetched from DB.
    
    # We can use service.migrate_table directly.
    
    # Get columns from MySQL
    cursor = mysql_client.connection.cursor()
    cursor.execute("SHOW COLUMNS FROM event_types")
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    config = MigrationConfig(
        batch_size=1000,
        schema=db_config.pg_schema,
        use_copy=True
    )
    
    result = await service.migrate_table("event_types", columns, config)
    
    assert result.success
    assert result.rows_migrated == 2
    assert len(result.errors) == 0
    
    # Verify data in Postgres
    conn = await asyncpg.connect(
        host=db_config.pg_host,
        port=db_config.pg_port,
        user=db_config.pg_user,
        password=db_config.pg_password,
        database=db_config.pg_db
    )
    
    rows = await conn.fetch(f"SELECT * FROM {db_config.pg_schema}.event_types")
    assert len(rows) == 2
    event_types = [r['event_type'] for r in rows]
    assert 'state_changed' in event_types
    assert 'call_service' in event_types
    
    await conn.close()
    await pg_client.close()
    mysql_client.close()
