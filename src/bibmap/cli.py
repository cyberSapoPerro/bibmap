import json
import re
import sqlite3

import requests
from tqdm import tqdm

TEST_DOI = "10.1103/physreve.87.032113"


def set_db_connection():
    conn = sqlite3.connect("db.sqlite")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def save_citations_json(citations, path="cites.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(citations, f, indent=2, ensure_ascii=False)


def opencitations_api(doi):
    url = f"https://opencitations.net/index/coci/api/v1/citations/{doi}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data
    

def crossref_api(doi):
    url = f"https://api.crossref.org/works/{doi}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    doi_data = data.get("message", {})
    return doi_data


def normalize_doi(doi: str) -> str:
    doi = doi.strip().lower()
    doi = re.sub(r'^https?://(dx\.)?doi\.org/', '', doi)
    doi = re.sub(r'^doi:\s*', '', doi)
    return doi


def fetch_jsons(doi):
    norm_doi = normalize_doi(doi)
    norm_doi = norm_doi.replace('/', '_')
    save_citations_json(crossref_api(norm_doi), path=f"crossref--{norm_doi}.json")
    save_citations_json(opencitations_api(norm_doi), path=f"opencitations--{norm_doi}.json")


def fetch_data(doi):
    doi = normalize_doi(doi)
    return {
        "crossref": crossref_api(doi),
        "opencitations": opencitations_api(doi)
    }


def transform_data(data, root_doi):
    papers = []
    citations = []
    authors = []
    paper_authors = []

    root_doi = normalize_doi(root_doi)

    crossref = data.get("crossref", {})

    papers.append({
        "doi": root_doi,
        "title": crossref.get("title", [""])[0],
        "reference_count": crossref.get("reference-count"),
        "publisher": crossref.get("publisher"),
        "container_title": (crossref.get("container-title") or [None])[0],
        "is_referenced_by_count": crossref.get("is-referenced-by-count"),
        "score": crossref.get("score"),
    })

    if crossref.get("author"):
        for a in crossref.get("author"):
            given = a.get("given")
            family = a.get("family")
            sequence = a.get("sequence")
            if not given or not family:
                continue
            authors.append({
                "given": given,
                "family": family
            })
            paper_authors.append({
                "paper_doi": root_doi,
                "given": given,
                "family": family,
                "sequence": sequence
            })

    for c in data["opencitations"]:
        citing_raw = c.get("citing")
        if not citing_raw:
            continue
        citing = normalize_doi(citing_raw)
        papers.append({
            "doi": citing,
            "title": None,
            "reference_count": None,
            "publisher": None,
            "container_title": None,
            "is_referenced_by_count": None,
            "score": None,
        })
        citations.append((citing, root_doi))

    for ref in crossref.get("reference", []):
        cited_raw = ref.get("DOI")
        if not cited_raw:
            continue
        cited = normalize_doi(cited_raw)
        papers.append({
            "doi": cited,
            "title": None,
            "reference_count": None,
            "publisher": None,
            "container_title": None,
            "is_referenced_by_count": None,
            "score": None,
        })
        citations.append((root_doi, cited))

    return papers, citations, authors, paper_authors


def upsert_papers(conn, papers):
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


def upsert_citations(conn, citations):
    conn.executemany("""
        INSERT OR IGNORE INTO citations (citing_doi, cited_doi)
        VALUES (?, ?)
    """, citations)


def upsert_authors(conn, authors):
    conn.executemany(
        """
        INSERT OR IGNORE INTO authors (given, family) VALUES (?, ?);
        """,
        [(a["given"], a["family"]) for a in authors]
    )


def upsert_paper_authors(conn, paper_authors):
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


def ingest_doi(conn, doi):
    doi = normalize_doi(doi)
    data = fetch_data(doi)
    papers, citations, authors, paper_authors = transform_data(data, doi)

    with conn:
        upsert_papers(conn, papers)
        upsert_citations(conn, citations)
        upsert_authors(conn, authors)
        upsert_paper_authors(conn, paper_authors)


def populate_db(conn):
    cur = conn.execute(
        """
        SELECT doi FROM papers WHERE title IS NULL
        """
    )
    dois = [row[0] for row in cur.fetchall()]
    for doi in tqdm(dois):
        ingest_doi(conn, doi)

    

def main():
    conn = set_db_connection()
    # ingest_doi(conn, TEST_DOI)
    populate_db(conn)


if __name__ == "__main__":
    main()
