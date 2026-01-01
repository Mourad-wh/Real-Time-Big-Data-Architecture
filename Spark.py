Script spark : from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, length, split, size
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import pandas_udf

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sentence_transformers import SentenceTransformer
import pandas as pd
import numpy as np

# =====================================
# 1. Spark Session
# =====================================
spark = SparkSession.builder \
    .appName("KafkaStreamingEnhancedAnalysis_BERT") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# 2. Kafka Schema (nouvelle structure)
# =====================================
schema = StructType([
    StructField("source", StringType(), True),
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("link", StringType(), True),
    StructField("by", StringType(), True),
    StructField("score", StringType(), True),
    StructField("time", StringType(), True),
    StructField("type", StringType(), True),
    StructField("text_combined", StringType(), True),
    StructField("category", StringType(), True),
    StructField("fetch_timestamp", StringType(), True)
])

# =====================================
# 3. Read Stream from Kafka
# =====================================
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.43.227:9092") \
    .option("subscribe", "rss_hn_feed") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# =====================================
# 4. Cleaning & Preprocessing
# =====================================
clean_df = parsed_df.withColumn(
    "text", col("text_combined")  # utiliser directement text_combined
).withColumn(
    "text_lower", lower(col("text"))
).withColumn(
    "word_count", size(split(col("text"), " "))
).withColumn(
    "text_length", length(col("text"))
)

# =====================================
# 5. Load HF Models (Driver side)
# =====================================
DEVICE = "cpu"

sent_tokenizer = AutoTokenizer.from_pretrained("finiteautomata/bertweet-base-sentiment-analysis")
sent_model = AutoModelForSequenceClassification.from_pretrained(
    "finiteautomata/bertweet-base-sentiment-analysis"
).to(DEVICE)

minilm_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device=DEVICE)

label_map = ["negative", "neutral", "positive"]

# =====================================
# 6. Pandas UDF for Sentiment
# =====================================
@pandas_udf(StringType())
def sentiment_udf(texts: pd.Series) -> pd.Series:
    inputs = sent_tokenizer(list(texts), padding=True, truncation=True, return_tensors="pt").to(DEVICE)
    with torch.no_grad():
        outputs = sent_model(**inputs).logits
    preds = torch.argmax(outputs, dim=1).cpu().numpy()
    return pd.Series([label_map[p] for p in preds])

# =====================================
# 7. Pandas UDF for Category + Similarity
# =====================================
CATEGORIES = {
    "AI": ["machine learning", "deep learning", "neural", "ai", "artificial intelligence"],
    "Cybersecurity": ["security", "hacking", "phishing", "malware"],
    "Finance": ["stock", "market", "price", "deal"],
    "Health": ["covid", "vaccine", "health", "virus"],
    "Sports": ["football", "nba", "soccer", "tennis"]
}

# Pre-encode category labels
category_embeddings = {
    cat: minilm_model.encode(" ".join(keywords), convert_to_tensor=True)
    for cat, keywords in CATEGORIES.items()
}

@pandas_udf(ArrayType(StringType()))
def category_udf(texts: pd.Series) -> pd.Series:
    embeddings = minilm_model.encode(list(texts), convert_to_tensor=True)
    categories_output = []
    for emb in embeddings:
        scores = {cat: float(np.dot(emb, vec) / (emb.norm() * vec.norm())) for cat, vec in category_embeddings.items()}
        top_cat = max(scores, key=scores.get)
        categories_output.append([top_cat])
    return pd.Series(categories_output)

# =====================================
# 8. Apply UDFs
# =====================================
final_df = clean_df \
    .withColumn("sentiment", sentiment_udf(col("text"))) \
    .withColumn("categories", category_udf(col("text")))

# =====================================
# 9. Output to Console
# =====================================
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()