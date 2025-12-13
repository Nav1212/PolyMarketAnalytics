from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType

# Define Silver Layer Schema for PolyMarket Data
silver_schema = StructType([
    StructField("market_id", StringType(), False),
    StructField("market_slug", StringType(), True),
    StructField("question", StringType(), True),
    StructField("description", StringType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("outcome_yes_price", DoubleType(), True),
    StructField("outcome_no_price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("liquidity", DoubleType(), True),
    StructField("active", BooleanType(), True),
    StructField("closed", BooleanType(), True),
    StructField("category", StringType(), True),
    StructField("tags", StringType(), True),  # JSON string or comma-separated
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("source", StringType(), True)
])