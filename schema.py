import sqlite3
import requests
import json
from datetime import datetime

def create_database():
    conn = sqlite3.connect("polymarket_events.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            slug TEXT UNIQUE,
            title TEXT,
            volume REAL,
            liquidity REAL,
            open_interest REAL,
            start_date TEXT,
            end_date TEXT,
            competitive REAL,
            created_at TEXT,
            closed BOOLEAN
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS markets (
            id INTEGER PRIMARY KEY,
            event_id INTEGER,
            question TEXT,
            condition_id TEXT,
            slug TEXT,
            volume REAL,
            liquidity REAL,
            outcomes TEXT,
            outcome_prices TEXT,
            best_bid REAL,
            best_ask REAL,
            last_trade_price REAL,
            FOREIGN KEY (event_id) REFERENCES events(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id TEXT PRIMARY KEY,
            taker_order_id TEXT,
            market TEXT,
            asset_id TEXT,
            side TEXT,
            size REAL,
            fee_rate_bps REAL,
            price REAL,
            status TEXT,
            match_time TEXT,
            last_update TEXT,
            outcome TEXT,
            maker_address TEXT,
            owner TEXT,
            transaction_hash TEXT,
            bucket_index INTEGER,
            type TEXT,
            FOREIGN KEY (market) REFERENCES markets(condition_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS maker_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT,
            order_id TEXT,
            maker_address TEXT,
            owner TEXT,
            matched_amount REAL,
            fee_rate_bps REAL,
            price REAL,
            asset_id TEXT,
            outcome TEXT,
            side TEXT,
            FOREIGN KEY (trade_id) REFERENCES trades(id)
        )
    ''')

    conn.commit()
    return conn

print(create_database())