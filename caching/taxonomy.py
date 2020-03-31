import requests

from config import SECRET_API_TOKEN, SECRET_PROD_URL

headers = {'content-type': 'application/json', 'x-auth-token': SECRET_API_TOKEN}


class PlatformError(Exception):
    """Raised when platform reponse contains 'error' key - that means any kind of internal error on platform site"""

    def __init__(self, message):
        self.message = message
        super().__init__(message)


def validate_models(data_source, taxons):
    data = {"subrequests": [
                {
                    "scope": {
                        "filters": {
                            "type": "group",
                            "logical_operator": "AND",
                            "clauses": [
                                {
                                    "type": "taxon_value",
                                    "operator": "=",
                                    "value": 1,
                                    "taxon": "company_id"
                                }
                            ]
                        }
                    },
                    "properties": {
                        "data_sources": data_source
                    },
                    "taxons": taxons
                }
            ]
        }
    resp = requests.post(url=f'{SECRET_PROD_URL}/validate', json=data, headers=headers)
    try:
        resp.raise_for_status()
        resp = resp.json()
    except:
        raise PlatformError(f'Taxonomy API request failed')

    return resp
