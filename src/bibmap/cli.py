import json
import re
import sqlite3

import requests

TEST_DOI = "10.1103/physreve.87.032113"


def set_db_connection():
    conn = sqlite3.connect("db.sqlite")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def save_citations_json(citations, path="cites.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(citations, f, indent=2, ensure_ascii=False)


# -- APIs ---
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
    save_citations_json(crossref_api(doi), path=f"crossref--{norm_doi}.json")
    save_citations_json(opencitations_api(doi), path=f"opencitations--{norm_doi}.json")


def fetch_data(doi):
    return {
        "crossref": crossref_api(doi),
        "opencitations": opencitations_api(doi)
    }


def transform_data(data, root_doi):
    papers = []
    citations = []

    root_doi = normalize_doi(root_doi)
    papers.append({
        "doi": root_doi,
        "title": data["crossref"].get("title", [""])[0]
    })

    for c in data["opencitations"]:
        citing_raw = c.get("citing")
        if not citing_raw:
            continue
        citing = normalize_doi(citing_raw)
        papers.append({"doi": citing, "title": None})
        citations.append((root_doi, citing))

    return papers, citations


def upsert_papers(conn, papers):
    conn.executemany(
        """
        INSERT INTO papers (doi, title)
        VALUES (?, ?)
        ON CONFLICT(doi) DO UPDATE SET
          title = COALESCE(excluded.title, papers.title)
        """, 
        [(p["doi"], p["title"]) for p in papers]
    )


def upsert_citations(conn, citations):
    conn.executemany("""
        INSERT OR IGNORE INTO citations (citing_doi, cited_doi)
        VALUES (?, ?)
    """, citations)


def ingest_doi(conn, doi):
    data = fetch_data(doi)
    papers, citations = transform_data(data, doi)

    with conn:
        upsert_papers(conn, papers)
        upsert_citations(conn, citations)
    

def main():
    conn = set_db_connection()
    ingest_doi(conn, TEST_DOI)


if __name__ == "__main__":
    main()
