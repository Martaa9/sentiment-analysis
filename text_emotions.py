from nrclex import NRCLex
import sqlite3


sqlite_db = 'sentiment.db'
conn = sqlite3.connect(sqlite_db)
cursor = conn.cursor()

cursor.execute("PRAGMA foreign_keys = ON;")
cursor.execute("PRAGMA journal_mode = OFF;")
cursor.execute("PRAGMA synchronous = OFF;")
cursor.execute("PRAGMA cache_size = 10000;")

review_emotions_table = "review_emotions"

cursor.execute(f'''
DROP TABLE IF EXISTS {review_emotions_table};
''')

cursor.execute(f'''
CREATE TABLE IF NOT EXISTS {review_emotions_table} (
	"parent_asin"	TEXT NOT NULL,
	"product"	TEXT NOT NULL,
	"rating"	NUMERIC NOT NULL,
	"title"	TEXT NOT NULL,
	"text"	TEXT NOT NULL,
    "fear"    NUMERIC NOT NULL,
    "anger"    NUMERIC NOT NULL,
    "anticipation"    NUMERIC NOT NULL,
    "trust"    NUMERIC NOT NULL,
    "surprise"    NUMERIC NOT NULL,
    "sadness"    NUMERIC NOT NULL,
    "disgust"    NUMERIC NOT NULL,
    "joy"    NUMERIC NOT NULL
);
''')

review_table = 'review'
product_table = 'product'

query = f'''
SELECT {review_table}.parent_asin, {review_table}.rating, {review_table}.title, {review_table}.text, {product_table}.title
FROM {review_table}
JOIN {product_table} ON {review_table}.parent_asin = {product_table}.parent_asin
'''
cursor.execute(query)

write_cursor = conn.cursor()

for row in cursor:
    (parent_asin, rating, title, text, product) = row
    text_to_analyse = str(title) + " " + str(text)
    emotions = NRCLex(text_to_analyse).raw_emotion_scores

    write_cursor.execute(f'''
                INSERT INTO {review_emotions_table} (parent_asin, product, rating, title, text,
                fear, anger, anticipation, trust, surprise, sadness, disgust, joy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (parent_asin, product, rating, title, text,
                  min(emotions.get('fear', 0), 1), min(emotions.get('anger', 0), 1), min(emotions.get('anticipation', 0), 1), min(emotions.get('trust', 0), 1), min(emotions.get('surprise', 0), 1), min(emotions.get('sadness', 0), 1), min(emotions.get('disgust', 0), 1), min(emotions.get('joy', 0), 1) ))

conn.commit()
conn.close()