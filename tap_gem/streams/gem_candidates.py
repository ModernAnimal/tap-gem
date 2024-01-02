import datetime
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone

import singer  # type: ignore

from tap_gem.streams.api import CANDIDATE_IDS, gem_api


def stream(api_key):
    logging.info("Started gem_candidates_pipeline.py")

    page_num = 1
    has_next = True

    while has_next:
        candidates, has_next = gem_api("candidates", api_key, page_num)
        for candidate in candidates:
            CANDIDATE_IDS.append(candidate["id"])
            singer.write_record(
                "gem_candidates",
                {
                    "id": candidate["id"],
                    "created_at": candidate["created_at"],
                    "created_by": candidate.get("created_by", None),
                    "company": candidate.get("company", None),
                    "first_name": candidate.get("first_name", None),
                    "last_name": candidate.get("last_name", None),
                    "linked_in_handle": candidate.get("linked_in_handle", None),
                    "location": candidate.get("location", None),
                    "school": candidate.get("school", None),
                    "sourced_from": candidate.get("sourced_from", None),
                    "phone_number": candidate.get("phone_number", None),
                    "title": candidate.get("title", None),
                    "weblink": candidate.get("weblink", None),
                    "emails": candidate.get("emails", None),
                },
            )

        page_num += 1

        logging.info("Gem candidates page completed %s", page_num)

    logging.info("Completed gem_candidates.py")
