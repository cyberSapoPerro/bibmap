import sqlite3


def fetch_paper_dois(conn: sqlite3.Connection) -> list:
    cur = conn.execute(
        """
        select doi from papers limit 5000;
        """
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

