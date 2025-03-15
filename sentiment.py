# from nltk.corpus import stopwords
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from textblob import TextBlob
from nltk.corpus import stopwords
# import spacy
import re
import sqlite3


# nltk.download('vader_lexicon')
# nltk.download('stopwords')
# nltk.download('punkt_tab')
# nltk.download('wordnet')


def clean_text(text: str):
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def tokenize_text(text: str):
    tokens = word_tokenize(text)
    tokens = [word.lower() for word in tokens if word.isalpha()]  # Conversion to lower characters and removing alpha-numeric characters
    stop_words = set(stopwords.words('english'))  # List of unique stop words
    tokens = [word for word in tokens if word not in stop_words]  # Removing stop words
    return tokens

def sentiment_analysis(tokens: list[str]):
    polarity_score = 0
    subjetive_score = 0
    count = 0
    
    for token in tokens:
        analysis = TextBlob(token)
        polarity_score += analysis.sentiment.polarity
        subjetive_score += analysis.sentiment.subjective
        count += 1

    # average_sentiment = (
    #     polarity = polarity_score / count if count > 0 else 0
    #     subjective = subjective_score / count if count > 0 else 0
    # )
    return polarity_score / count if count > 0 else 0


sia = SentimentIntensityAnalyzer()
sqlite_db = 'sentiment.db'
review_table = 'review'
product_table = 'product'
review_sentiment_table = 'review_sentiment'
review_emotions_table = "review_emotions"
conn = sqlite3.connect(sqlite_db)
read_cursor = conn.cursor()
write_cursor = conn.cursor()

write_cursor.execute("PRAGMA foreign_keys = ON;")
write_cursor.execute("PRAGMA journal_mode = OFF;")
write_cursor.execute("PRAGMA synchronous = OFF;")
write_cursor.execute("PRAGMA cache_size = 10000;")

write_cursor.execute(f'''
DROP TABLE IF EXISTS {review_sentiment_table};
''')

write_cursor.execute(f'''
CREATE TABLE IF NOT EXISTS {review_sentiment_table} (
    "asin"	TEXT NOT NULL,
	"parent_asin"	TEXT NOT NULL,
	"rating"	NUMERIC NOT NULL,
	"product"	TEXT NOT NULL,
	"title"	TEXT NOT NULL,
	"text"	TEXT NOT NULL,
    "negative_sentiment"    NUMERIC NOT NULL,
    "neutral_sentiment"    NUMERIC NOT NULL,
    "positive_sentiment"    NUMERIC NOT NULL,
    "compound_sentiment"    NUMERIC NOT NULL,
    "polarity_sentiment"    NUMERIC NOT NULL,
    "subjective_sentiment"    NUMERIC NOT NULL
);
''')


query = f'''
SELECT {review_table}.asin, {review_table}.parent_asin, {review_table}.rating, {review_table}.title, {review_table}.text, {product_table}.title
FROM {review_table}
JOIN {product_table} ON {review_table}.parent_asin = {product_table}.parent_asin
'''
read_cursor.execute(query)

for row in read_cursor:
    (asin, parent_asin, rating, title, text, product) = row
    text_to_analyse = str(title) + " " + str(text)
    cleaned_text = clean_text(text_to_analyse)

    sentiment = sia.polarity_scores(cleaned_text)
    blob = TextBlob(cleaned_text)
    polarity, subjective = blob.sentiment

    write_cursor.execute(f'''
                INSERT INTO {review_sentiment_table} (asin, parent_asin, rating, product, title, text,
                negative_sentiment, neutral_sentiment, positive_sentiment, compound_sentiment, polarity_sentiment, subjective_sentiment)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (asin, parent_asin, rating, product, title, text, sentiment.get("neg"), sentiment.get("neu"), sentiment.get("pos"), sentiment.get("compound"), polarity, subjective))
    conn.commit()

conn.commit()
conn.close()