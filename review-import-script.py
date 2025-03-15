import sqlite3
import orjson

# Define the path to your JSONL file and SQLite database
jsonl_file = 'Appliances.jsonl'
sqlite_db = 'sentiment.db'
table_name = 'review'
join_table_name = 'product'

# Connect to the SQLite database
conn = sqlite3.connect(sqlite_db)
cursor = conn.cursor()
cursor.execute("PRAGMA foreign_keys = ON;")
cursor.execute("PRAGMA journal_mode = OFF;")
cursor.execute("PRAGMA synchronous = OFF;")
cursor.execute("PRAGMA cache_size = 10000;")

# Create a table in the database (adjust the schema as needed)
cursor.execute(f'''
DROP TABLE IF EXISTS {table_name};
''')

cursor.execute(f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    "asin"	TEXT NOT NULL,
    "parent_asin"	TEXT NOT NULL,
    "rating"	NUMERIC NOT NULL,
    "title"	TEXT NOT NULL,
    "text"	TEXT NOT NULL,
    "user_id"	TEXT NOT NULL,
    "timestamp"	INTEGER NOT NULL,
    "helpful_vote"	INTEGER NOT NULL DEFAULT 0,
    "verified_purchase"	INTEGER,
    UNIQUE(user_id, timestamp)
    FOREIGN KEY("parent_asin") REFERENCES {join_table_name}("parent_asin")
);
''')

def product_exists(id):
    cursor.execute(f'SELECT 1 FROM {join_table_name} WHERE parent_asin = ?', (id,))
    exist = cursor.fetchone() is not None
    return exist


def review_exists(timestamp: int, user_id: str):
    cursor.execute(f'SELECT 1 FROM {table_name} WHERE user_id = ? AND timestamp = ?', (user_id, timestamp))
    exist = cursor.fetchone() is not None
    return exist

# Open the JSONL file and read it line by line
# with open(jsonl_file, 'r') as f:
#     for line in f:
#         # Parse each line as JSON
#         data = orjson.loads(line)
#
#         # Insert the data into the SQLite table
#         if product_exists(data['parent_asin']) is False:
#             continue
#
#         cursor.execute(f'''
#         INSERT OR IGNORE INTO {table_name} (asin, parent_asin, title, rating, text, user_id, timestamp, helpful_vote, verified_purchase) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
#         ''', (data['asin'], data['parent_asin'], data['title'], data['rating'], data['text'], data['user_id'], data['timestamp'], data['helpful_vote'], data['verified_purchase']))
#         conn.commit()

# Commit the transaction and close the connection
conn.commit()
conn.close()