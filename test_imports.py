#!/usr/bin/env python3
"""Test script to verify basic imports and functionality."""

import asyncio
from migrate.config import DBConfig
from migrate.database.mysql_client import MySQLClient
from migrate.database.pg_client import PGClient
from migrate.services.migration_service import MigrationService
from migrate.services.validation_service import ValidationService

def test_imports():
    """Test that all main modules can be imported."""
    print("Testing imports...")
    
    # Test config import
    print("✓ DBConfig imported")
    
    # Test database clients
    print("✓ MySQLClient imported")
    print("✓ PGClient imported")
    
    # Test services
    print("✓ MigrationService imported")
    print("✓ ValidationService imported")
    
    print("All imports successful!")

async def test_config_loading():
    """Test that config can be loaded."""
    print("\nTesting config loading...")
    try:
        # This should load from .env.example if no .env exists
        config = DBConfig()
        print("✓ DBConfig loaded successfully")
        return True
    except Exception as e:
        print(f"✗ Config loading failed: {e}")
        return False

if __name__ == "__main__":
    test_imports()
    asyncio.run(test_config_loading())
