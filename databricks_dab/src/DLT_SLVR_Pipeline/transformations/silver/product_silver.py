from pyspark import pipelines as dp
from pyspark.sql.functions import *

#creating empty stream table
dp.create_streaming_table(
  name="external_cat.silver_ecom.sl_products"
)

dp.create_auto_cdc_flow(
  source="bz_products",
  target="external_cat.silver_ecom.sl_products",
  keys= ["product_id"],
  sequence_by="last_update_ts",
  stored_as_scd_type= 2
)

@dp.table(
  name="external_cat.silver_ecom.sl_vw_products"
)
def sl_vw_products():
  return(spark.read.table("external_cat.silver_ecom.sl_products").filter(col("__END_AT").isNull()))
