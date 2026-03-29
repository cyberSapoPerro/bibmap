import sqlite3

from tqdm import tqdm

from bibmap.db.queries import fetch_imcomplete_papers
from bibmap.utils import normalize_doi
from bibmap.api.data_transformation import (
    transform_crossref_data,
    transform_opencitations_data,
)


def upsert_papers(conn: sqlite3.Connection, papers: list) -> None:
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


def upsert_citations(conn: sqlite3.Connection, citations: list) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO citations (citing_doi, cited_doi)
        VALUES (?, ?)
        """,
        citations,
    )


def upsert_authors(conn: sqlite3.Connection, authors: list) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO authors (given, family) VALUES (?, ?);
        """,
        [(a["given"], a["family"]) for a in authors],
    )


def upsert_paper_authors(conn: sqlite3.Connection, paper_authors: list) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO paper_author (
            paper_doi, author_given, author_family, sequence
        )
        VALUES (?, ?, ?, ?)
        """,
        [
            (
                i.get("paper_doi"),
                i.get("given"),
                i.get("family"),
                i.get("sequence")
            )
            for i in paper_authors
        ],
    )


def ingest_data_by_doi_from_crossref(conn: sqlite3.Connection, doi: str) -> None:
    doi = normalize_doi(doi)
    papers, citations, authors, paper_authors = transform_crossref_data(doi)

    with conn:
        upsert_papers(conn, papers)
        upsert_citations(conn, citations)
        upsert_authors(conn, authors)
        upsert_paper_authors(conn, paper_authors)


def ingest_data_by_doi_from_opencitations(conn: sqlite3.Connection, doi: str) -> None:
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
    ingest_data_by_doi_from_crossref(conn, doi)
    ingest_data_by_doi_from_opencitations(conn, doi)


def enrich_papers_with_metadata(conn: sqlite3.Connection, limit: int = 10000) -> None:
    dois = fetch_imcomplete_papers(conn, limit)

    total = len(dois)
    print(f"Completing {total} incomplete papers with metadata (limit={limit})...")
    for doi in tqdm(dois, desc="Fetching metadata"):
        ingest_data_by_doi_from_crossref(conn, doi)


def enrich_papers_with_metadata_and_citations(conn: sqlite3.Connection, limit: int = 10000) -> None:
    dois = fetch_imcomplete_papers(conn, limit)

    total = len(dois)
    print(f"Enriching {total} incomplete papers with metadata and citations (limit={limit})...")

    for doi in tqdm(dois, desc="Fetching metadata"):
        ingest_data_by_doi_from_crossref(conn, doi)
        ingest_data_by_doi_from_opencitations(conn, doi)
