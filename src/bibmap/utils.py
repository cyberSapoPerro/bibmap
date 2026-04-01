import re


def normalize_doi(doi: str) -> str:
    """Normalize a DOI string to a canonical form.

    Strips whitespace, converts to lowercase, and removes common
    URL prefixes and DOI prefixes.

    Args:
        doi: The DOI string to normalize.

    Returns:
        The normalized DOI string.
    """
    doi = doi.strip().split()[0]
    doi = doi.strip().lower()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi)
    doi = re.sub(r"^doi:\s*", "", doi)
    return doi
