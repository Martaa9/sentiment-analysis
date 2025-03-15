import sqlite3
import re
from collections import Counter
from nltk.corpus import stopwords

sqlite_db = 'sentiment.db'
word_count_table = 'word_counts'
conn = sqlite3.connect(sqlite_db)
cur = conn.cursor()

cur.execute('SELECT text FROM review')
reviews = cur.fetchall()
word_counter = Counter()

cur.execute(f'''
DROP TABLE IF EXISTS {word_count_table};
''')

cur.execute(f'''
    CREATE TABLE IF NOT EXISTS {word_count_table} (
        word TEXT PRIMARY KEY,
        count INTEGER
    )
''')

def is_integer(text):
    try:
        int(text)
        return True
    except ValueError:
        return False


for review in reviews:
    # text toxenization with conversion to lower characters and removing alphanumerical characters
    words = re.findall(r'\w+', review[0].lower())
    word_counter.update(words)

stop_words = set(stopwords.words('english'))  # list of unique stop words
# Writing words count to database
for word, count in word_counter.most_common():
    if word not in stop_words and is_integer(word) is False:
        cur.execute('INSERT OR REPLACE INTO word_counts (word, count) VALUES (?, ?)', (word, count))

conn.commit()
cur.close()
conn.close()