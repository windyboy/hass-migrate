import typer

from . import migrate as migrate_cmd, schema, utils, validate

# Create main app
app = typer.Typer(help="Home Assistant MySQL → PostgreSQL migration tool")

# Create sub-command groups
schema_app = typer.Typer(help="Manage PostgreSQL schema")
app.add_typer(schema_app, name="schema")

migrate_app = typer.Typer(help="Migrate data from MySQL to PostgreSQL")
app.add_typer(migrate_app, name="migrate")

validate_app = typer.Typer(help="Validate migration integrity", invoke_without_command=True)
app.add_typer(validate_app, name="validate")

# Register all commands
schema.register_schema_commands(schema_app)
utils.register_utils_commands(app)
validate.register_validate_commands(validate_app)
migrate_cmd.register_migrate_commands(migrate_app)

__all__ = ["app"]

