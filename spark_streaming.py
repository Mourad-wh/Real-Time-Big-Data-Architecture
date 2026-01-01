from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, length, split, size, current_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import pandas_udf
import pandas as pd
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sentence_transformers import SentenceTransformer

# =====================================
# 1. Spark Session
# =====================================
spark = SparkSession.builder \
    .appName("KafkaStreamingWithOutput") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✓ Spark Session initialized")

# =====================================
# 2. Kafka Schema (Input)
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
# 3. Read Stream from Kafka (INPUT)
# =====================================
KAFKA_BROKER = "192.168.43.227:9092"  # 🔥 CHANGE avec ton IP VM1
INPUT_TOPIC = "rss_hn_feed"
OUTPUT_TOPIC = "analyzed_results"  # 🔥 NOUVEAU

print(f"📡 Connecting to Kafka: {KAFKA_BROKER}")
print(f"📝 Input Topic: {INPUT_TOPIC}")
print(f"📤 Output Topic: {OUTPUT_TOPIC}\n")

kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "100") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

print("✓ Kafka input stream connected")

# =====================================
# 4. Data Cleaning
# =====================================
clean_df = parsed_df \
    .withColumn("text", col("text_combined")) \
    .withColumn("text_lower", lower(col("text"))) \
    .withColumn("word_count", size(split(col("text"), " "))) \
    .withColumn("text_length", length(col("text"))) \
    .withColumn("processing_time", current_timestamp()) \
    .filter(col("text_length") > 10)

# =====================================
# 5. Load ML Models
# =====================================
DEVICE = "cpu"
print(f"🤖 Loading ML models on {DEVICE}...")

# Sentiment Model - Utiliser SafeTensors
sent_tokenizer = AutoTokenizer.from_pretrained(
    "finiteautomata/bertweet-base-sentiment-analysis"
)
sent_model = AutoModelForSequenceClassification.from_pretrained(
    "finiteautomata/bertweet-base-sentiment-analysis",
    use_safetensors=True  # 🔥 AJOUT - Force SafeTensors
).to(DEVICE)
sent_model.eval()

# Embedding Model - Utiliser SafeTensors
minilm_model = SentenceTransformer(
    "sentence-transformers/all-MiniLM-L6-v2", 
    device=DEVICE,
    use_safetensors=True  # 🔥 AJOUT - Force SafeTensors
)

label_map = ["negative", "neutral", "positive"]
print("✓ Models loaded successfully\n")

# =====================================
# 6. Pandas UDF for Sentiment
# =====================================
@pandas_udf(StringType())
def sentiment_udf(texts: pd.Series) -> pd.Series:
    if texts.empty:
        return pd.Series([], dtype=str)
    
    texts_truncated = texts.apply(lambda x: x[:512] if isinstance(x, str) else "")
    
    inputs = sent_tokenizer(
        list(texts_truncated), 
        padding=True, 
        truncation=True, 
        max_length=128,
        return_tensors="pt"
    ).to(DEVICE)
    
    with torch.no_grad():
        outputs = sent_model(**inputs).logits
    
    preds = torch.argmax(outputs, dim=1).cpu().numpy()
    return pd.Series([label_map[p] for p in preds])

# =====================================
# 7. Pandas UDF for Categorization
# =====================================
CATEGORIES = {
    "AI & Machine Learning": [
        "machine learning", "deep learning", "neural", "ai", 
        "artificial intelligence", "transformer", "llm", "gpt"
    ],
    "Cybersecurity": [
        "security", "hacking", "phishing", "malware", 
        "breach", "vulnerability", "exploit", "encryption"
    ],
    "Finance & Crypto": [
        "stock", "market", "price", "deal", "bitcoin", 
        "crypto", "trading", "investment"
    ],
    "Health & Medicine": [
        "covid", "vaccine", "health", "virus", "medical", 
        "disease", "treatment", "pandemic"
    ],
    "Technology": [
        "programming", "software", "developer", "code", 
        "github", "cloud", "api", "framework"
    ],
    "Science": [
        "research", "study", "discovery", "experiment", 
        "quantum", "physics", "biology"
    ]
}

print("🔍 Computing category embeddings...")
category_embeddings = {
    cat: minilm_model.encode(" ".join(keywords), convert_to_tensor=True)
    for cat, keywords in CATEGORIES.items()
}
print("✓ Category embeddings ready\n")

@pandas_udf(StringType())
def category_udf(texts: pd.Series) -> pd.Series:
    if texts.empty:
        return pd.Series([], dtype=str)
    
    texts_truncated = texts.apply(lambda x: x[:512] if isinstance(x, str) else "")
    
    embeddings = minilm_model.encode(list(texts_truncated), convert_to_tensor=True)
    categories_output = []
    
    for emb in embeddings:
        scores = {
            cat: float(torch.nn.functional.cosine_similarity(emb.unsqueeze(0), vec.unsqueeze(0)))
            for cat, vec in category_embeddings.items()
        }
        top_cat = max(scores, key=scores.get)
        categories_output.append(top_cat)
    
    return pd.Series(categories_output)

# =====================================
# 8. Apply ML Analysis
# =====================================
print("🔄 Starting streaming analysis...\n")

analyzed_df = clean_df \
    .withColumn("sentiment", sentiment_udf(col("text"))) \
    .withColumn("predicted_category", category_udf(col("text")))

# Sélectionner les colonnes pour l'output
output_df = analyzed_df.select(
    "source",
    "id",
    "title",
    "link",
    "by",
    "sentiment",
    "predicted_category",
    "word_count",
    "text_length",
    "processing_time"
)

# =====================================
# 9. Write to BOTH Console AND Kafka
# =====================================

# 9.1 - Afficher dans la console (pour debug)
console_query = output_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 3) \
    .trigger(processingTime='10 seconds') \
    .start()

print("✓ Console output started")

# 9.2 - Écrire dans Kafka (pour le dashboard)
# Convertir DataFrame en JSON
kafka_output_df = output_df.select(
    to_json(struct("*")).alias("value")
)

kafka_query = kafka_output_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", OUTPUT_TOPIC) \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .trigger(processingTime='10 seconds') \
    .start()

print("✓ Kafka output started")

print("=" * 80)
print("🎯 Spark Streaming is now processing data")
print("=" * 80)
print(f"📥 Reading from: {INPUT_TOPIC}")
print(f"📤 Writing to: {OUTPUT_TOPIC}")
print(f"🤖 Models: BERTweet (sentiment) + MiniLM (category)")
print(f"📊 Categories: {', '.join(CATEGORIES.keys())}")
print("=" * 80)
print("\n✅ Results are being sent to Kafka topic: analyzed_results")
print("✅ Dashboard can now read from this topic!")
print("\nPress Ctrl+C to stop...\n")

# Attendre les deux queries
console_query.awaitTermination()