from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
from nltk.corpus import stopwords
from pyspark.sql.types import StringType, ArrayType, MapType
from pyspark.sql.functions import col, udf

# This is a class which have all the natural language processing functions.
# this class is used to perform sentiment analysis and tokenize the words.
# it uses NLTK python library for tha

nltk.download('vader_lexicon')
nltk.download('stopwords')
nltk.download('punkt')
sia = SentimentIntensityAnalyzer()
stop_words = set(stopwords.words('english'))


@udf(StringType())
def get_sentiment(text):
    """
    This method will return the sentiment of the text.
    It will use the nltk vader sentiment analyzer.
    it will return the sentiment as positive, negative or neutral.
    """
    sentiment_score = sia.polarity_scores(text)["compound"]
    if sentiment_score >= 0.05:
        return "positive"
    elif sentiment_score <= -0.05:
        return "negative"
    else:
        return "neutral"


@udf(ArrayType(StringType()))
def tokenize_and_get_top_words(text, sample_size=0.0001):
    """
    This method will tokenize the text and will return the top 10 words.
    it will also remove the stop words to capture the most important words.
    """
    tokens = word_tokenize(text)
    tokens = [word.lower() for word in tokens if word.isalpha()]
    tokens = [word for word in tokens if word not in stop_words]
    freq_dist = FreqDist(tokens)
    top_words = [word  for word, k in freq_dist.most_common(10)]
    return top_words

