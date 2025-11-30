# from pyspark import pipelines as dp
# from pyspark.sql.functions import current_timestamp

# filename = spark.conf.get("pipelines.filename")

# #creating intermediate view as source
# @dp.table(
#   name=f"bz_{filename}"
# )
# def bz_processing(filename=filename):
#   return(spark.readStream\
#     .format("cloudFiles")\
#     .option("cloudFiles.format", "parquet")\
#     .option("cloudFiles.schemaLocation", f"abfss://bronze@deepanshde.dfs.core.windows.net/checkpoints/{filename}")\
#     .option("cloudFiles.inferColumnTypes","true")\
#     .load(f"abfss://raw@deepanshde.dfs.core.windows.net/{filename}")\
#     .withColumn("last_update_ts", current_timestamp())
#   )
  



