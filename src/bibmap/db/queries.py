import sqlite3

from typing import Optional


def fetch_paper_by_doi(conn: sqlite3.Connection, doi: str) -> Optional[tuple]:
    """Fetch a paper from the database by its DOI.

    Args:
        conn: SQLite database connection.
        doi: The DOI of the paper to fetch.

    Returns:
        A tuple containing the paper row data, or None if not found.
    """
    cur = conn.execute(
        """
        SELECT * FROM papers WHERE doi = ?;
        """,
        (doi,),
    )
    return cur.fetchone()


def fetch_paper_dois(conn: sqlite3.Connection, limit: int = 1000) -> list[str]:
    """Fetch a list of DOIs from the papers table.

    Args:
        conn: SQLite database connection.
        limit: Maximum number of DOIs to return.

    Returns:
        A list of DOIs.
    """
    cur = conn.execute(
        """
        select doi from papers limit ?;
        """,
        (limit,),
    )
    dois = [row[0] for row in cur.fetchall()]
    return dois


def fetch_citation_edges_for_nodes(
    conn: sqlite3.Connection, nodes: list[str]
) -> list[tuple[str, str]]:
    """Fetch citation edges for given nodes.

    Args:
        conn: SQLite database connection.
        nodes: List of DOIs to fetch citations for.

    Returns:
        A list of tuples containing (citing_doi, cited_doi).
    """
    placeholders = ",".join(["?"] * len(nodes))
    query = f"""
        SELECT * FROM citations
        WHERE citing_doi IN ({placeholders})
           OR cited_doi IN ({placeholders});
    """
    cur = conn.execute(query, nodes + nodes)
    return cur.fetchall()


def collect_nodes_from_edges(edges: list[tuple[str, str]]) -> list[str]:
    """Extract unique nodes from citation edges.

    Args:
        edges: List of tuples containing (citing_doi, cited_doi).

    Returns:
        A list of unique DOIs found in the edges.
    """
    nodes: set[str] = set()
    for citing, cited in edges:
        nodes.add(citing)
        nodes.add(cited)
    return list(nodes)


def fetch_incomplete_papers(conn: sqlite3.Connection, limit: int = 10000) -> list[str]:
    """Fetch DOIs of papers with missing title information.

    Args:
        conn: SQLite database connection.
        limit: Maximum number of DOIs to return.

    Returns:
        A list of DOIs with NULL title.
    """
    cur = conn.execute(
        """
        SELECT doi FROM papers WHERE title IS NULL LIMIT ?
        """,
        (limit,),
    )
    dois = [row[0] for row in cur.fetchall()]
    return dois


def fetch_cited_and_citing_dois(conn: sqlite3.Connection, root_doi: str) -> set[str]:
    """Retrieve DOIs of papers citing and cited by a given root paper.

    Args:
        conn (sqlite3.Connection): Active SQLite database connection.
        root_doi (str): DOI of the root paper.

    Returns:
        list[str]: DOIs of papers that either cite the root paper or are cited by it.
    """
    query = """
        SELECT * FROM citations
        WHERE citing_doi = ?
           OR cited_doi = ?;
    """
    cur = conn.execute(query, (root_doi, root_doi))
    rows = cur.fetchall()
    dois = set()
    for citing, cited in rows:
        dois.add(citing)
        dois.add(cited)
    return dois
