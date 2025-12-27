from sentiment_extract import SentimentExtract
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import *
import logging

# spark = sparknlp.start(apple_silicon=True)

sentiment_service = SentimentExtract(
    input_col="comments",
    model_name="sentimentdl_use_twitter",
    encoder_name="tfhub_use",
    gpu=False,
    apple_silicon=True
)

# Test cases
test_sentences = [
    "I love this product, it works perfectly!",
    "This is the worst experience I have ever had.",
    "The delivery was okay, nothing special.",
    "---,,,",
    "           "
]

print("\n--- Sentiment Results ---")
for text in test_sentences:
    prediction = sentiment_service.predict(text)
    print(f"Text: {text}")
    print(f"Sentiment: {prediction}\n")