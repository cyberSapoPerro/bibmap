import os
import sqlite3


def set_db_connection():
    base_dir = os.path.expanduser("~/.local/share/bibman")
    os.makedirs(base_dir, exist_ok=True)

    db_path = os.path.join(base_dir, "db.sqlite")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    return conn
