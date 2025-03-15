import sqlite3
import orjson

# Define the path to your JSONL file and SQLite database
jsonl_file = 'meta_Appliances.jsonl'
sqlite_db = 'sentiment.db'
table_name = 'product'
category = 'Appliances'

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
	"parent_asin"	TEXT NOT NULL UNIQUE,
	"main_category"	TEXT NOT NULL,
	"title"	TEXT,
	"rating_number"	NUMERIC NOT NULL,
	"average_rating"	NUMERIC NOT NULL,
	"store"	TEXT,
	PRIMARY KEY("parent_asin")
);
''')

# Open the JSONL file and read it line by line
with open(jsonl_file, 'r') as f:
    entries = []

    for line in f:
        # Parse each line as JSON
        data = orjson.loads(line)

        if data['main_category'] is None or data['main_category'] != category:
            continue
        
        # Insert the data into the SQLite table
        cursor.execute(f'''
        INSERT INTO {table_name} (parent_asin, main_category, title, rating_number, average_rating, store) VALUES (?, ?, ?, ?, ?, ?)
        ''', (data['parent_asin'], data['main_category'], data['title'], data['rating_number'], data['average_rating'], data['store']))

# Commit the transaction and close the connection
conn.commit()
conn.close()