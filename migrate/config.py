from dotenv import load_dotenv
import os

load_dotenv()

class DBConfig:
    def __init__(self):
        self.mysql_host = os.getenv("MYSQL_HOST")
        self.mysql_port = int(os.getenv("MYSQL_PORT", 3306))
        self.mysql_user = os.getenv("MYSQL_USER")
        self.mysql_password = os.getenv("MYSQL_PASSWORD")
        self.mysql_db = os.getenv("MYSQL_DB")

        self.pg_host = os.getenv("PG_HOST")
        self.pg_port = int(os.getenv("PG_PORT", 5432))
        self.pg_user = os.getenv("PG_USER")
        self.pg_password = os.getenv("PG_PASSWORD")
        self.pg_db = os.getenv("PG_DB")

