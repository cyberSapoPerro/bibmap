import json
import re

import requests

TEST_DOI = "10.1103/physreve.87.032113"


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


## -- DB ---
def get_jsons(doi):
    norm_doi = normalize_doi(doi)
    norm_doi = norm_doi.replace('/', '_')
    save_citations_json(crossref_api(doi), path=f"crossref--{norm_doi}.json")
    save_citations_json(opencitations_api(doi), path=f"opencitations--{norm_doi}.json")
    

def main():
    get_jsons(TEST_DOI)


if __name__ == "__main__":
    main()
