from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.table(
  name="bz_customers"
)
def bz_customers():
  return(spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", f"abfss://bronze@deepanshde.dfs.core.windows.net/checkpoints/customers")\
    .option("cloudFiles.inferColumnTypes","true")\
    .load(f"abfss://raw@deepanshde.dfs.core.windows.net/customers")\
    .withColumn("last_update_ts", current_timestamp())
  )