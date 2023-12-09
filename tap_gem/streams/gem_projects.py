import datetime
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone

import singer  # type: ignore

from tap_gem.streams.api import PROJECT_IDS, gem_api


def stream(api_key):
    logging.info("Started gem_projects.py")

    page_num = 1
    has_next = True

    try:
        while has_next:
            projects, has_next = gem_api("projects", api_key, page_num)
            for project in projects:
                PROJECT_IDS.append(project["id"])
                singer.write_record(
                    "gem_projects",
                    {
                        "id": project["id"],
                        "created_at": project["created_at"],
                        "user_id": project.get("user_id", None),
                        "name": project.get("name", None),
                        "privacy_type": project.get("privacy_type", None),
                        "description": project.get("description", None),
                        "is_archived": project.get("is_archived", None)
                    },
                )
            page_num += 1
            logging.info("Gem projects page completed %s", page_num)

    except Exception as e:
        logging.exception(e)

    logging.info("Completed gem_projects.py")
