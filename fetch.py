# filter_events_excluding_tags.py
import requests
from typing import Iterable, List, Dict, Optional

LIMIT, INCREMENT = 10, 100

EVENTS_URL = "https://gamma-api.polymarket.com/events"
TAGS_BY_SLUG_URL = "https://gamma-api.polymarket.com/tags/slug/{slug}"
LIST_TAGS_URL = "https://gamma-api.polymarket.com/tags"

result = []

def fetch_events(order_field="startDate", ascending=False, closed=False):
    valid, offset = 0, 0
    while valid < LIMIT:
        params = {
            "order": order_field,
            "ascending": str(ascending).lower(),
            "closed": str(closed).lower(),
            "limit": INCREMENT,
            "offset": offset,
        }
        response = requests.get(EVENTS_URL, params=params)
        response.raise_for_status()
        
        for event in response.json():
            if "Up or Down" in event.get("title", "") or event.get("volume", 0) < 10000:
                continue

            valid += 1
            result.append(event)


        offset += INCREMENT

    print("Valid events fetched:")
    for event in result:
        print(event["title"])
        print(event["startDate"])
        print(event["volume"])
        print(event["slug"])

fetch_events()
