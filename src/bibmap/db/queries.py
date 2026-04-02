import sqlite3

from typing import Optional, Set, List


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


def fetch_dois_if_not_metadata(
    conn: sqlite3.Connection,
    dois: List[str],
) -> Set[str]:
    """Return DOIs whose metadata is missing (i.e., title is NULL).

    Args:
        conn: SQLite database connection.
        dois: Iterable of DOIs to check.

    Returns:
        Set of DOIs with no metadata.
    """

    placeholders = ",".join("?" for _ in dois)

    query = f"""
        SELECT doi
        FROM papers
        WHERE doi IN ({placeholders})
        AND title IS NULL
        ;
    """

    cur = conn.execute(query, dois)
    rows = cur.fetchall()

    return {doi for (doi,) in rows}


def fetch_citation_graph_data(
    conn: sqlite3.Connection,
    root_doi: str,
    depth: int = 1,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Fetch citation graph data starting from a root DOI.

    Performs a breadth-first search to retrieve all connected papers
    (papers that cite or are cited by the root DOI, recursively).

    Args:
        conn: SQLite database connection.
        root_doi: The starting DOI for the graph traversal.
        depth: Maximum depth for BFS traversal. Defaults to 1.

    Returns:
        A tuple of (nodes, edges):
            - nodes: List of unique DOIs in the graph.
            - edges: List of (citing_doi, cited_doi) tuples.
    """
    all_nodes: list[str] = [root_doi]
    all_edges: list[tuple[str, str]] = []

    current_level = [root_doi]

    for _ in range(depth):
        if not current_level:
            break

        new_dois: set[str] = set()
        new_edges: list[tuple[str, str]] = []

        for doi in current_level:
            edges = fetch_citation_edges_for_nodes(conn, [doi])
            for citing, cited in edges:
                new_edges.append((citing, cited))
                if citing != doi:
                    new_dois.add(citing)
                if cited != doi:
                    new_dois.add(cited)

        if not new_dois:
            break

        all_edges.extend(new_edges)

        for citing, cited in new_edges:
            if citing not in all_nodes:
                all_nodes.append(citing)
            if cited not in all_nodes:
                all_nodes.append(cited)

        current_level = list(new_dois)

    return all_nodes, all_edges
