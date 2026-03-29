import sqlite3

from typing import Optional, Tuple


def fetch_paper_by_doi(conn: sqlite3.Connection, doi: str) -> Optional[Tuple]:
    cur = conn.execute(
        """
        SELECT * FROM papers WHERE doi = ?;
        """,
        (doi,)
    )
    return cur.fetchone()

def fetch_paper_dois(conn: sqlite3.Connection, limit:int = 1000) -> list:
    cur = conn.execute(
        """
        select doi from papers limit ?;
        """,
        (limit,)
    )
    dois = [row[0] for row in cur.fetchall()]
    return dois


def fetch_citation_edges_for_nodes(conn: sqlite3.Connection, nodes: list) -> list:
    placeholders = ",".join(["?"] * len(nodes))
    query = f"""
        SELECT * FROM citations
        WHERE citing_doi IN ({placeholders})
           OR cited_doi IN ({placeholders});
    """
    cur = conn.execute(query, nodes + nodes)
    return cur.fetchall()


def collect_nodes_from_edges(edges: list) -> list:
    nodes = set()
    for citing, cited in edges:
        nodes.add(citing)
        nodes.add(cited)
    return list(nodes)       

def fetch_imcomplete_papers(
    conn: sqlite3.Connection,
    limit: int = 10000
) -> list:
    cur = conn.execute(
        """
        SELECT doi FROM papers WHERE title IS NULL LIMIT ?
        """,
        (limit,)
    )
    dois = [row[0] for row in cur.fetchall()]
    return dois
