from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name="external_cat.gold_ecom.gl_orders"
)
def gl_orders():
    order_df      = spark.read.table("external_cat.silver_ecom.sl_vw_orders")
    customer_df   = spark.read.table("external_cat.silver_ecom.sl_vw_customers")
    product_df    = spark.read.table("external_cat.silver_ecom.sl_vw_products")

    order_agg_df = order_df.join(customer_df, order_df.customer_id == customer_df.customer_id,"inner")\
                            .join(product_df, order_df.product_id == product_df.product_id)\
                            .select(
                                order_df.order_id,
                                order_df.order_date,
                                order_df.customer_id,
                                concat(customer_df.first_name,lit(" "),customer_df.last_name).alias("name"),
                                customer_df.email,
                                customer_df.city,
                                customer_df.state,
                                order_df.product_id,
                                product_df.category,
                                product_df.product_name,
                                product_df.price,
                                product_df.brand,
                                order_df.quantity,
                                order_df.total_amount
                            )
    return order_agg_df

@dp.table(
      name = "external_cat.gold_ecom.gl_brand_wise_sales"
)
def gl_brand_wise_sales():
    order_df = spark.read.table("external_cat.gold_ecom.gl_orders")
    brand_wise_sales_df = order_df.groupBy("brand").agg(round(sum("total_amount"),2).alias("total_sales"))
    return brand_wise_sales_df

@dp.table(
      name = "external_cat.gold_ecom.gl_city_wise_sales"
)
def gl_city_wise_sales():
    order_df = spark.read.table("external_cat.gold_ecom.gl_orders")
    brand_wise_sales_df = order_df.groupBy("city").agg(round(sum("total_amount"),2).alias("total_sales"))
    return brand_wise_sales_df


