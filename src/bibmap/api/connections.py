import requests


from bibmap.utils import normalize_doi

def fetch_opencitations(doi: str) -> dict:
    doi = normalize_doi(doi)
    url = f"https://opencitations.net/index/coci/api/v1/citations/{doi}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data

    except requests.exceptions.HTTPError as e:
        print(f"[Opencitations HTTP error] DOI={doi} | {e}")

    except requests.exceptions.RequestException as e:
        # Covers connection errors, timeouts, DNS, etc.
        print(f"[Opencitations request failed] DOI={doi} | {e}")

    except ValueError as e:
        # JSON decoding error
        print(f"[Opencitations invalid JSON] DOI={doi} | {e}")

    return {}
    

def fetch_crossref(doi: str) -> dict:
    doi = normalize_doi(doi)
    url = f"https://api.crossref.org/works/{doi}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("message", {})

    except requests.exceptions.HTTPError as e:
        print(f"[Crossref HTTP error] DOI={doi} | {e}")

    except requests.exceptions.RequestException as e:
        # Covers connection errors, timeouts, DNS, etc.
        print(f"[Crossref request failed] DOI={doi} | {e}")

    except ValueError as e:
        # JSON decoding error
        print(f"[Crossref invalid JSON] DOI={doi} | {e}")

    return {}

def fetch_paper_data(doi: str) -> dict:
    doi = normalize_doi(doi)
    return {
        "crossref": fetch_crossref(doi),
        "opencitations": fetch_opencitations(doi)
    }
