import sqlite3

from tqdm import tqdm

from bibmap.utils import normalize_doi
from bibmap.api.data_transformation import (
    transform_crossref_data,
    transform_opencitations_data,
)
from bibmap.db.queries import (
    fetch_incomplete_papers,
    fetch_dois_if_not_metadata,
)


def upsert_papers(conn: sqlite3.Connection, papers: list[dict]) -> None:
    """Insert or update papers in the database.

    Args:
        conn: SQLite database connection.
        papers: List of paper dictionaries with keys: doi, title,
            reference_count, publisher, container_title,
            is_referenced_by_count, score, published.
    """
    conn.executemany(
        """
        INSERT INTO papers (
            doi, title, reference_count, publisher,
            container_title, is_referenced_by_count, score, published
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(doi) DO 
            UPDATE SET
            title = COALESCE(excluded.title, papers.title),
            reference_count = COALESCE(excluded.reference_count, papers.reference_count),
            publisher = COALESCE(excluded.publisher, papers.publisher),
            container_title = COALESCE(excluded.container_title, papers.container_title),
            is_referenced_by_count = COALESCE(excluded.is_referenced_by_count, papers.is_referenced_by_count),
            score = COALESCE(excluded.score, papers.score),
            published = COALESCE(excluded.published, papers.published)
        """,
        [
            (
                p.get("doi"),
                p.get("title"),
                p.get("reference_count"),
                p.get("publisher"),
                p.get("container_title"),
                p.get("is_referenced_by_count"),
                p.get("score"),
                p.get("published"),
            )
            for p in papers
        ],
    )


def upsert_citations(
    conn: sqlite3.Connection, citations: list[tuple[str, str]]
) -> None:
    """Insert citation relationships into the database.

    Args:
        conn: SQLite database connection.
        citations: List of tuples containing (citing_doi, cited_doi).
    """
    conn.executemany(
        """
        INSERT OR IGNORE INTO citations (citing_doi, cited_doi)
        VALUES (?, ?)
        """,
        citations,
    )


def upsert_authors(conn: sqlite3.Connection, authors: list[dict]) -> None:
    """Insert authors into the database.

    Args:
        conn: SQLite database connection.
        authors: List of author dictionaries with keys: given, family.
    """
    conn.executemany(
        """
        INSERT OR IGNORE INTO authors (given, family) VALUES (?, ?);
        """,
        [(a["given"], a["family"]) for a in authors],
    )


def upsert_paper_authors(conn: sqlite3.Connection, paper_authors: list[dict]) -> None:
    """Insert paper-author relationships into the database.

    Args:
        conn: SQLite database connection.
        paper_authors: List of dictionaries with keys: paper_doi,
            given, family, sequence.
    """
    conn.executemany(
        """
        INSERT OR IGNORE INTO paper_author (
            paper_doi, author_given, author_family, sequence
        )
        VALUES (?, ?, ?, ?)
        """,
        [
            (i.get("paper_doi"), i.get("given"), i.get("family"), i.get("sequence"))
            for i in paper_authors
        ],
    )


def ingest_data_by_doi_from_crossref(conn: sqlite3.Connection, doi: str) -> None:
    """Ingest paper metadata from Crossref for a given DOI.

    Args:
        conn: SQLite database connection.
        doi: The DOI of the paper to ingest.
    """
    doi = normalize_doi(doi)
    papers, citations, authors, paper_authors = transform_crossref_data(doi)

    with conn:
        upsert_papers(conn, papers)
        upsert_citations(conn, citations)
        upsert_authors(conn, authors)
        upsert_paper_authors(conn, paper_authors)


def ingest_data_by_doi_from_opencitations(conn: sqlite3.Connection, doi: str) -> None:
    """Ingest citation data from OpenCitations for a given DOI.

    Args:
        conn: SQLite database connection.
        doi: The DOI of the paper to ingest citations for.
    """
    doi = normalize_doi(doi)
    papers, citations = transform_opencitations_data(doi)

    cited_paper = {
        "doi": doi,
        "title": None,
        "reference_count": None,
        "publisher": None,
        "container_title": None,
        "is_referenced_by_count": None,
        "score": None,
        "published": None,
    }

    with conn:
        upsert_papers(conn, [cited_paper] + papers)
        upsert_citations(conn, citations)


def populate_database_from_one_doi(conn: sqlite3.Connection, doi: str) -> None:
    """Populate the database with all available data for a single DOI.

    Combines data from both Crossref and OpenCitations.

    Args:
        conn: SQLite database connection.
        doi: The DOI of the paper to populate.
    """
    ingest_data_by_doi_from_crossref(conn, doi)
    ingest_data_by_doi_from_opencitations(conn, doi)


def enrich_random_papers_with_metadata(
    conn: sqlite3.Connection, limit: int = 10000
) -> None:
    """Enrich random incomplete papers with metadata from Crossref.

    Args:
        conn: SQLite database connection.
        limit: Maximum number of papers to enrich.
    """
    dois = fetch_incomplete_papers(conn, limit)

    total = len(dois)
    print(f"Completing {total} incomplete papers with metadata (limit={limit})...")
    for doi in tqdm(dois, desc="Fetching metadata"):
        ingest_data_by_doi_from_crossref(conn, doi)


def enrich_random_papers_with_metadata_and_citations(
    conn: sqlite3.Connection, limit: int = 10000
) -> None:
    """Enrich random incomplete papers with both metadata and citations.

    Args:
        conn: SQLite database connection.
        limit: Maximum number of papers to enrich.
    """
    dois = fetch_incomplete_papers(conn, limit)

    total = len(dois)
    print(
        f"Enriching {total} incomplete papers with metadata and citations (limit={limit})..."
    )

    for doi in tqdm(dois, desc="Fetching metadata"):
        ingest_data_by_doi_from_crossref(conn, doi)
        ingest_data_by_doi_from_opencitations(conn, doi)


def enrich_specific_papers_with_metadata(conn: sqlite3.Connection, dois: set) -> None:
    """Enrich a list of incomplete papers with metadata from Crossref.

    Args:
        conn: SQLite database connection.
        dois: Set of dois to be enriched.
    """

    total = len(dois)
    print(f"Completing {total} incomplete papers with metadata...")
    for doi in tqdm(dois, desc="Fetching metadata"):
        ingest_data_by_doi_from_crossref(conn, doi)


def enrich_graph_dois(conn: sqlite3.Connection, dois: list[str]) -> None:
    """Enrich papers in a graph with Crossref metadata

    Args:
        conn: SQLite database connection.
        dois: DOIs in the graph.
    """
    dois_with_not_metadata = fetch_dois_if_not_metadata(conn, dois)
    enrich_specific_papers_with_metadata(conn, dois_with_not_metadata)
