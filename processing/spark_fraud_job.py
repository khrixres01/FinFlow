from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Spark Session ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName('FinFlow - Fraud Detection') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

print('='*50)
print('FinFlow Fraud Detection Job Started')
print('='*50)

# ── Read Clean Data from Staging ─────────────────────────
print('Reading clean transactions from staging...')

df = spark.read \
    .parquet('hdfs://namenode:9000/data/staging/clean/')

print('Records loaded: {}'.format(df.count()))

# ── Initialize fraud columns ──────────────────────────────
df = df \
    .withColumn('is_flagged',  F.lit(False)) \
    .withColumn('flag_reason', F.lit(None).cast('string'))

# ── Rule 1: Large Amount ──────────────────────────────────
print('Applying Rule 1: Large amount detection...')

df = df.withColumn('is_flagged',
    F.when(F.col('amount') > 1000000, True)
     .otherwise(F.col('is_flagged'))
).withColumn('flag_reason',
    F.when(F.col('amount') > 1000000, 'HIGH_VALUE_TRANSACTION')
     .otherwise(F.col('flag_reason'))
)

# ── Rule 2: Off Hours Transaction ────────────────────────
print('Applying Rule 2: Off hours detection...')

df = df.withColumn('is_flagged',
    F.when(
        (F.col('transaction_hour') >= 1) &
        (F.col('transaction_hour') <= 4),
        True
    ).otherwise(F.col('is_flagged'))
).withColumn('flag_reason',
    F.when(
        (F.col('transaction_hour') >= 1) &
        (F.col('transaction_hour') <= 4),
        'OFF_HOURS_TRANSACTION'
    ).otherwise(F.col('flag_reason'))
)

# ── Rule 3: Round Amount ──────────────────────────────────
print('Applying Rule 3: Round amount detection...')

df = df.withColumn('is_flagged',
    F.when(
        (F.col('amount') > 100000) &
        (F.col('amount') % 1000 == 0),
        True
    ).otherwise(F.col('is_flagged'))
).withColumn('flag_reason',
    F.when(
        (F.col('amount') > 100000) &
        (F.col('amount') % 1000 == 0),
        'ROUND_AMOUNT'
    ).otherwise(F.col('flag_reason'))
)

# ── Rule 4: High Velocity ─────────────────────────────────
print('Applying Rule 4: High velocity detection...')

window_1min = Window \
    .partitionBy('account_from') \
    .orderBy(F.col('timestamp').cast('long')) \
    .rangeBetween(-60, 0)

df = df.withColumn('txn_count_1min',
    F.count('transaction_id').over(window_1min)
)

df = df.withColumn('is_flagged',
    F.when(F.col('txn_count_1min') > 5, True)
     .otherwise(F.col('is_flagged'))
).withColumn('flag_reason',
    F.when(F.col('txn_count_1min') > 5, 'HIGH_VELOCITY')
     .otherwise(F.col('flag_reason'))
)

# ── Rule 5: Multiple Failed Attempts ─────────────────────
print('Applying Rule 5: Multiple failures detection...')

window_5min = Window \
    .partitionBy('account_from') \
    .orderBy(F.col('timestamp').cast('long')) \
    .rangeBetween(-300, 0)

df = df.withColumn('fail_count_5min',
    F.count(
        F.when(F.col('status') == 'failed',
               F.col('transaction_id'))
    ).over(window_5min)
)

df = df.withColumn('is_flagged',
    F.when(F.col('fail_count_5min') > 3, True)
     .otherwise(F.col('is_flagged'))
).withColumn('flag_reason',
    F.when(F.col('fail_count_5min') > 3, 'MULTIPLE_FAILURES')
     .otherwise(F.col('flag_reason'))
)

# ── Clean up helper columns ───────────────────────────────
df = df.drop('txn_count_1min', 'fail_count_5min')

# ── Summary ───────────────────────────────────────────────
total        = df.count()
flagged      = df.filter(F.col('is_flagged') == True).count()
clean        = total - flagged
flag_rate    = round(flagged * 100.0 / total, 2)

print('\n' + '='*50)
print('FRAUD DETECTION SUMMARY')
print('='*50)
print('Total transactions:  {}'.format(total))
print('Flagged:             {} ({}%)'.format(flagged, flag_rate))
print('Clean:               {}'.format(clean))
print('='*50)

# ── Breakdown by flag reason ──────────────────────────────
print('\nBreakdown by flag reason:')
df.filter(F.col('is_flagged') == True) \
  .groupBy('flag_reason') \
  .count() \
  .orderBy(F.col('count').desc()) \
  .show()

# ── Show sample flagged transactions ─────────────────────
print('Sample flagged transactions:')
df.filter(F.col('is_flagged') == True) \
  .select('transaction_id', 'amount', 'channel',
          'status', 'flag_reason') \
  .show(5, truncate=False)

# ── Write flagged data to HDFS ────────────────────────────
print('Writing flagged data to HDFS...')

df.write \
  .mode('overwrite') \
  .partitionBy('transaction_date') \
  .parquet('hdfs://namenode:9000/data/staging/flagged/')

print('Flagged data written to HDFS!')
print('Job complete!')

spark.stop()