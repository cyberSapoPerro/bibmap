import sqlite3

from tqdm import tqdm

from utils import normalize_doi
from api.connections import fetch_paper_data
from api.data_transformation import transform_data



def upsert_papers(conn: sqlite3.Connection, papers: list) -> None:
    conn.executemany(
        """
        INSERT INTO papers (
            doi, title, reference_count, publisher,
            container_title, is_referenced_by_count, score
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(doi) DO 
            UPDATE SET
            title = COALESCE(excluded.title, papers.title),
            reference_count = COALESCE(excluded.reference_count, papers.reference_count),
            publisher = COALESCE(excluded.publisher, papers.publisher),
            container_title = COALESCE(excluded.container_title, papers.container_title),
            is_referenced_by_count = COALESCE(excluded.is_referenced_by_count, papers.is_referenced_by_count),
            score = COALESCE(excluded.score, papers.score)
        """, 
        [(
            p["doi"],
            p["title"],
            p["reference_count"],
            p["publisher"],
            p["container_title"],
            p["is_referenced_by_count"],
            p["score"],
        ) for p in papers]
    )


def upsert_citations(conn: sqlite3.Connection, citations: list) -> None:
    conn.executemany("""
        INSERT OR IGNORE INTO citations (citing_doi, cited_doi)
        VALUES (?, ?)
    """, citations)


def upsert_authors(conn: sqlite3.Connection, authors: list) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO authors (given, family) VALUES (?, ?);
        """,
        [(a["given"], a["family"]) for a in authors]
    )


def upsert_paper_authors(conn: sqlite3.Connection, paper_authors: list) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO paper_author (
            paper_doi, author_given, author_family, sequence
        )
        VALUES (?, ?, ?, ?)
        """, 
        [(
            i["paper_doi"],
            i["given"],
            i["family"],
            i["sequence"]
        ) for i in paper_authors ]
    )


def ingest_paper_by_doi(conn: sqlite3.Connection, doi: str) -> None:
    doi = normalize_doi(doi)
    data = fetch_paper_data(doi)
    papers, citations, authors, paper_authors = transform_data(data, doi)

    with conn:
        upsert_papers(conn, papers)
        upsert_citations(conn, citations)
        upsert_authors(conn, authors)
        upsert_paper_authors(conn, paper_authors)


def enrich_incomplete_papers(conn: sqlite3.Connection, limit: int) -> None:
    cur = conn.execute(
        """
        SELECT doi FROM papers WHERE title IS NULL LIMIT ?
        """,
        (limit,)
    )
    dois = [row[0] for row in cur.fetchall()]

    total = len(dois)
    print(f"Enriching {total} incomplete papers (limit={limit})...")

    for doi in tqdm(dois, desc="Fetching metadata"):
        ingest_paper_by_doi(conn, doi)
