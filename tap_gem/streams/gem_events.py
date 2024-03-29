from concurrent.futures import ThreadPoolExecutor
import datetime
from itertools import repeat
import logging
import requests
import time

import singer  # type: ignore

from tap_gem.streams.api import CANDIDATE_IDS

# setting cutoff for records created after certain date - looking back one
# week to capture any records missed in the event of errors in previous runs.
cutoff = datetime.datetime.now() - datetime.timedelta(days=7)
cutoff = int(time.mktime(cutoff.timetuple()))


def get_events(api_key, candidate_id):
    # set API key
    headers = {
        "X-API-Key": api_key,
        "Content-type": "application/json",
    }

    for attempt in range(3):
        try:
            events_url = f"https://api.gem.com/v0/candidates/{candidate_id}/events?created_after={cutoff}&page_size=100"
            response = requests.get(events_url, headers=headers, timeout=120)
            if response.status_code != 200:
                response_events = []
            else:
                response_events = response.json()
        except Exception as e:
            logging.exception(f"Error occurred: {e}")
        else:
            break

    return response_events


def parse_events(response_events):
    parsed_records = []
    for i in response_events:
        if len(i) == 0:
            continue
        else:
            singer.write_record(
                "gem_events",
                {
                    "id": i["id"],
                    "created_at": i["timestamp"],
                    "candidate_id": i.get("candidate_id", None),
                    "contact_medium": i.get("contact_medium", None),
                    "user_id": i.get("user_id", None),
                    "on_behalf_of_user_id": i.get("on_behalf_of_user_id", None),
                    "type": i.get("type", None),
                    "subtype": i.get("subtype", None),
                    "reply_status": i.get("reply_status", None),
                },
            )

    return parsed_records


def process_batch(candidate_id, api_key):
    # Use candidate_id from candidates job as input for API call
    events_api_response = get_events(api_key, candidate_id)

    # Parse API payload into tuples
    parse_events(events_api_response)

    logging.info("Gem Events - candidate %s completed", candidate_id)


def stream(api_key):
    logging.info("Started gem_events_pipeline.py")

    with ThreadPoolExecutor(max_workers=10) as executor:
        for _ in executor.map(process_batch, CANDIDATE_IDS, repeat(api_key)):
            pass

    logging.info("Completed gem_events_pipeline.py")
