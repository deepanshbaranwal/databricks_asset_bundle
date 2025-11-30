from pyspark import pipelines as dp
from pyspark.sql.functions import *

#creating empty stream table
dp.create_streaming_table(
  name="external_cat.silver_ecom.sl_regions"
)

dp.create_auto_cdc_flow(
  source="bz_regions",
  target="external_cat.silver_ecom.sl_regions",
  keys= ["region_id"],
  sequence_by="last_update_ts",
  stored_as_scd_type= 2
)

@dp.table(
  name="external_cat.silver_ecom.sl_vw_regions"
)
def sl_vw_regions():
  return(spark.read.table("external_cat.silver_ecom.sl_regions").filter(col("__END_AT").isNull()))
