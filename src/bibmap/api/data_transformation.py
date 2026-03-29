from datetime import datetime

from utils import normalize_doi


def extract_date(obj):
    if not obj:
        return None
    parts = obj.get("date-parts")
    y, m, d = (parts [0] + [1, 1, 1])[:3]
    return datetime(y, m, d).date()


def transform_data(data:  dict, root_doi: str) -> tuple:
    papers = []
    citations = []
    authors = []
    paper_authors = []

    crossref = data.get("crossref", {})
    root_doi = normalize_doi(root_doi)
    opencitations = data.get("opencitations", {})

    papers.append({
        "doi": root_doi,
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
                "paper_doi": root_doi,
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
        citations.append((root_doi, cited))

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
        citations.append((citing, root_doi))

    return (papers, citations, authors, paper_authors)
