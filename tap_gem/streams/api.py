import json
from typing import List

import requests

CANDIDATE_IDS: List[str] = []
PROJECT_IDS: List[str] = []


def gem_api(resource_path, api_key, page_num):
    headers = {
        "X-API-Key": api_key,
        "Content-type": "application/json",
    }

    paginated_url = (
        f"https://api.gem.com/v0/{resource_path}?page={page_num}&page_size=100"
    )
    page_response = requests.get(paginated_url, headers=headers)
    response_pagination = json.loads(page_response.headers["X-Pagination"])
    has_next = page_num < response_pagination["total_pages"]

    return page_response.json(), has_next
