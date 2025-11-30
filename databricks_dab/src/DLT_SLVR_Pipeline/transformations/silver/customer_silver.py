from pyspark import pipelines as dp
from pyspark.sql.functions import *

rules_dict = {
    "rule_1": "customer_id IS NOT NULL",
    "rule_2": "email IS NOT NULL",
    "rule_3": "REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')"
}

dp.create_streaming_table(
  name="external_cat.silver_ecom.sl_customers",
  expect_all_or_drop=rules_dict
)

dp.create_auto_cdc_flow(
  source="bz_customers",
  target="external_cat.silver_ecom.sl_customers",
  keys=["customer_id"],
  sequence_by="last_update_ts",
  stored_as_scd_type=2
)

@dp.table(
  name="external_cat.silver_ecom.sl_vw_customers"
)
def sl_vw_customers():
  return spark.read.table("external_cat.silver_ecom.sl_customers").filter(col("__END_AT").isNull())