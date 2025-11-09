import sqlite3


class DatabaseDao:
    def __init__(self):
        self.conn = sqlite3.connect("polymarket_events.db")
        self.cursor = self.conn.cursor()

    def close(self):
        self.conn.close()
    
    def commit(self):
        self.conn.commit()

    def create_database(self):

        self.cursor.execute('''
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

        self.cursor.execute('''
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

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proxy_wallet TEXT,
                side TEXT,
                asset TEXT,
                condition_id TEXT,
                size REAL,
                price REAL,
                timestamp INTEGER,
                title TEXT,
                outcome TEXT,
                outcome_index INTEGER,
                transaction_hash TEXT,
                FOREIGN KEY (condition_id) REFERENCES markets(condition_id)
            )
        ''')

        self.conn.commit()
        return self.conn
    
    def insert_event(self, event):

        self.cursor.execute('''
            INSERT OR REPLACE INTO events 
            (id, slug, title, volume, liquidity, open_interest, start_date, end_date, competitive, created_at, closed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
        event['id'],
        event.get('slug'),
        event.get('title'),
        event.get('volume', 0),
        event.get('liquidity', 0),
        event.get('openInterest', 0),
        event.get('startDate'),
        event.get('endDate'),
        event.get('competitive', 0),
        event.get('createdAt'),
        event.get('closed', False)
    ))

        

    def insert_market(self, event_id, market):
        self.cursor.execute('''
            INSERT OR REPLACE INTO markets 
            (id, event_id, question, condition_id, slug, volume, liquidity, 
             outcomes, outcome_prices, best_bid, best_ask, last_trade_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            market['id'],
            event_id,
            market.get('question'),
            market.get('conditionId'),
            market.get('slug'),
            market.get('volumeNum', 0),
            market.get('liquidityNum', 0),
            market.get('outcomes'),
            market.get('outcomePrices'),
            market.get('bestBid'),
            market.get('bestAssk'),
            market.get('lastTradePrice')
        ))

    def insert_trade(self, trade):
        """Insert trade from Data API /trades endpoint"""
        self.cursor.execute('''
            INSERT OR IGNORE INTO trades 
            (proxy_wallet, side, asset, condition_id, size, price, timestamp, 
             title, outcome, outcome_index, transaction_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            trade.get('proxyWallet'),
            trade.get('side'),
            trade.get('asset'),
            trade.get('conditionId'),
            trade.get('size', 0),
            trade.get('price', 0),
            trade.get('timestamp', 0),
            trade.get('title'),
            trade.get('outcome'),
            trade.get('outcomeIndex'),
            trade.get('transactionHash')
        ))

