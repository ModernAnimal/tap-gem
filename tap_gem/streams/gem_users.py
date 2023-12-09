import datetime
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone

import singer  # type: ignore

from tap_gem.streams.api import gem_api


def stream(api_key):
    logging.info("Started gem_candidates_pipeline.py")

    page_num = 1
    has_next = True

    try:
        while has_next:
            users, has_next = gem_api("users", api_key, page_num)
            for user in users:
                singer.write_record(
                    "gem_users",
                    {
                        "id": user["id"],
                        "name": user.get("name", None),
                        "email": user.get("email", None)
                    },
                )
            page_num += 1
            logging.info("Gem users page completed %s", page_num)

    except Exception as e:
        logging.exception(e)

    logging.info("Completed gem_projects.py")
