from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.table(
  name="bz_products"
)
def bz_customers():
  return(spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", f"abfss://bronze@deepanshde.dfs.core.windows.net/checkpoints/products")\
    .option("cloudFiles.inferColumnTypes","true")\
    .load(f"abfss://raw@deepanshde.dfs.core.windows.net/products")\
    .withColumn("last_update_ts", current_timestamp())
  )