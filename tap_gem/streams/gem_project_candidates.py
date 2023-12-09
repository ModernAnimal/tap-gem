import logging

import singer  # type: ignore

from tap_gem.streams.api import PROJECT_IDS, gem_api


def stream(api_key):
    logging.info("Started gem_project_candidates.py")

    page_num = 1
    has_next = True

    for project_id in PROJECT_IDS:
        while has_next:
            projects, has_next = gem_api(
                f"projects/{project_id}/candidates", api_key, page_num
            )
            for project in projects:
                singer.write_record(
                    "gem_project_candidates",
                    {
                        "id": project_id + '|' + project["candidate_id"],
                        "project_id": project_id,
                        "candidate_id": project["candidate_id"],
                        "added_at": project.get("added_at", None)
                    },
                )
            page_num += 1
            logging.info("Gem page completed %s", page_num)

    logging.info("Completed gem_project_candidates.py")
