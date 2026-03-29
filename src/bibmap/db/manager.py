from pathlib import Path
from importlib import resources
import sqlite3


def set_db_connection() -> sqlite3.Connection:
    base_dir = Path.home() / ".local" / "share" / "bibmap"
    db_path = base_dir / "db.sqlite"

    base_dir.mkdir(parents=True, exist_ok=True)
    db_exists = db_path.exists()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    if not db_exists:
        schema_sql = resources.files("bibmap.db") \
            .joinpath("schema.sql") \
            .read_text(encoding="utf-8")

        conn.executescript(schema_sql)

    return conn
