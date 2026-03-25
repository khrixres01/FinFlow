from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

# ── Spark Session ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName('FinFlow - Transaction Cleaner') \
    .config('spark.sql.warehouse.dir', '/data/warehouse') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

print('='*50)
print('FinFlow PySpark Cleaning Job Started')
print('='*50)

# ── Define Schema ─────────────────────────────────────────
schema = StructType([
    StructField('transaction_id',   StringType(),    True),
    StructField('timestamp',        StringType(),    True),
    StructField('customer_id',      StringType(),    True),
    StructField('account_from',     StringType(),    True),
    StructField('account_to',       StringType(),    True),
    StructField('amount',           DoubleType(),    True),
    StructField('transaction_type', StringType(),    True),
    StructField('channel',          StringType(),    True),
    StructField('status',           StringType(),    True),
    StructField('city',             StringType(),    True),
])

# ── Read Raw Data from HDFS ───────────────────────────────
print('Reading raw transactions from HDFS...')

raw_df = spark.read \
    .option('header', 'true') \
    .schema(schema) \
    .csv('hdfs://namenode:9000/data/raw/transactions/')

print('Raw records loaded: {}'.format(raw_df.count()))
raw_df.printSchema()

# ── Step 1: Remove nulls ──────────────────────────────────
print('Step 1: Removing null rows...')

clean_df = raw_df.dropna(
    subset=['transaction_id', 'amount', 'customer_id',
            'account_from', 'account_to']
)
print('After null removal: {}'.format(clean_df.count()))

# ── Step 2: Fix data types ────────────────────────────────
print('Step 2: Fixing data types...')

clean_df = clean_df \
    .withColumn('timestamp',
        F.to_timestamp(F.col('timestamp'), 'yyyy-MM-dd HH:mm:ss')
    ) \
    .withColumn('amount',
        F.round(F.col('amount').cast(DoubleType()), 2)
    ) \
    .withColumn('transaction_type',
        F.lower(F.col('transaction_type'))
    ) \
    .withColumn('channel',
        F.lower(F.col('channel'))
    ) \
    .withColumn('status',
        F.lower(F.col('status'))
    )

# ── Step 3: Validate business rules ──────────────────────
print('Step 3: Validating business rules...')

# Remove negative or zero amounts
clean_df = clean_df.filter(F.col('amount') > 0)

# Remove where source = destination
clean_df = clean_df.filter(
    F.col('account_from') != F.col('account_to')
)

# Keep only valid statuses
valid_statuses = ['success', 'failed', 'pending']
clean_df = clean_df.filter(
    F.col('status').isin(valid_statuses)
)

# Keep only valid channels
valid_channels = ['mobile', 'atm', 'web', 'branch']
clean_df = clean_df.filter(
    F.col('channel').isin(valid_channels)
)

print('After validation: {}'.format(clean_df.count()))

# ── Step 4: Add metadata columns ─────────────────────────
print('Step 4: Adding metadata columns...')

clean_df = clean_df \
    .withColumn('processed_at',    F.current_timestamp()) \
    .withColumn('transaction_date', F.to_date(F.col('timestamp'))) \
    .withColumn('transaction_hour', F.hour(F.col('timestamp')))

# ── Step 5: Show sample ───────────────────────────────────
print('\nSample cleaned records:')
clean_df.show(5, truncate=False)

print('Schema after cleaning:')
clean_df.printSchema()

# ── Step 6: Write to HDFS staging ────────────────────────
print('Writing clean data to HDFS staging...')

clean_df.write \
    .mode('overwrite') \
    .partitionBy('transaction_date') \
    .parquet('hdfs://namenode:9000/data/staging/clean/')

print('Clean data written successfully!')
print('Total clean records: {}'.format(clean_df.count()))
print('='*50)
print('Cleaning job complete!')
print('='*50)

spark.stop()