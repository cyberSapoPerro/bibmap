from datetime import datetime

from bibmap.api.connections import fetch_crossref, fetch_opencitations
from bibmap.utils import normalize_doi


def transform_crossref_data(doi: str) -> tuple:
    papers = []
    citations = []
    authors = []
    paper_authors = []

    crossref = fetch_crossref(doi)

    def extract_date(obj):
        if not obj:
            return None
        parts = obj.get("date-parts")
        y, m, d = (parts [0] + [1, 1, 1])[:3]
        return datetime(y, m, d).date()

    papers.append({
        "doi": doi,
        "title": crossref.get("title", [""])[0],
        "reference_count": crossref.get("reference-count"),
        "publisher": crossref.get("publisher"),
        "container_title": (crossref.get("container-title") or [None])[0],
        "is_referenced_by_count": crossref.get("is-referenced-by-count"),
        "score": crossref.get("score"),
        "published": extract_date(crossref.get("published"))
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
                "paper_doi": doi,
                "given": given,
                "family": family,
                "sequence": sequence
            })

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
            "published": None
        })
        citations.append((doi, cited))
    return (papers, citations, authors, paper_authors)


def transform_opencitations_data(doi: str) -> tuple:
    opencitations = fetch_opencitations(doi)
    papers = []
    citations = []

    for c in opencitations:
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
            "published": None
        })
        citations.append((citing, doi))

    return (papers, citations)
