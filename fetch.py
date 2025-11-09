from time import time
from schema import DatabaseDao
from py_clob_client.clob_types import TradeParams
from py_clob_client.client import ClobClient
from dotenv import load_dotenv
import os
import datetime
import requests
import json

load_dotenv()

EVENTS_URL = "https://gamma-api.polymarket.com/events"
TRADES_URL = "https://data-api.polymarket.com/trades"

COMMIT_BATCH_SIZE = 10

result = []

# client = ClobClient("https://clob.polymarket.com", key=os.getenv("POLYMARKET_API_KEY"), chain_id=137)
# client.set_api_creds(client.create_or_derive_api_creds())
databaseDao = DatabaseDao()

def fetch_trades_for_market(condition_id):
    all_trades = []
    current_time, limit = 0, 500

    while True:
        batch_trades, offset = [], 0

        while offset <= 1000:
            params = {
                "market": [condition_id],
                "limit": limit,
                "offset": offset
            }

            if current_time > 0:
                params["after"] = current_time

            response = requests.get(TRADES_URL, params=params)
            response.raise_for_status()

            trades = response.json()
            if not trades or len(trades) == 0: break

            batch_trades.extend(trades)

            for trade in trades:
                databaseDao.insert_trade(trade)
            
            if len(trades) < limit: break

            offset += limit
        
        if not batch_trades:
            # No more trades found
            break

        all_trades.extend(batch_trades)
        print(f"Fetched {len(batch_trades)} trades for market {condition_id} (total: {len(all_trades)})")

        if len(batch_trades) >= 1500:
            # Move time window forward to the timestamp of the last trade
            last_trade_time = batch_trades[-1].get('timestamp', 0)
            
            if last_trade_time <= current_time:
                # Safety check - prevent infinite loop
                print(f"Warning: No progress on time window for market {condition_id}")
                break
            
            current_time = last_trade_time
            print(f"Moving to next time window starting at {datetime.fromtimestamp(current_time)}")
        else:
            # We got fewer than 1500, meaning we've fetched all trades
            break


def fetch_and_store(LIMIT, INCREMENT):
    valid, offset = 0, 0

    while valid < LIMIT:
        # Parameters for Events API
        params = {
            "order": "startDate",
            "ascending": "false",
            "limit": INCREMENT,
            "offset": offset
        }

        event_response = requests.get(EVENTS_URL, params=params)
        event_response.raise_for_status()

        event_data = event_response.json()
        for event in event_data:
            event_title = event.get("title", "")
            if "Up or Down" in event_title or "above" in event_title or event.get("volume", 0) < 5000:
                continue
            
            databaseDao.insert_event(event)

            for market in event.get('markets', []):
                databaseDao.insert_market(event['id'], market)
                fetch_trades_for_market(market.get('conditionId'))

            valid += 1

            if valid % COMMIT_BATCH_SIZE == 0:
                databaseDao.commit()
                print(f"Committed {valid} valid events to the database.")
        
        offset += INCREMENT

    databaseDao.commit()
    databaseDao.close()
    print(f"Stored {valid} valid events.")
            


def fetch_events(order_field="startDate", ascending=False, closed=False, LIMIT=100, INCREMENT=50):
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

    print("Valid events fetched")
    with open("events.json", "a") as f:
        json.dump(result, f, indent=2)
    for event in result:
        print(event["title"])
        print(event["startDate"])
        print(event["volume"])
        print(event["slug"])


databaseDao.create_database()
fetch_and_store(1000, 200)