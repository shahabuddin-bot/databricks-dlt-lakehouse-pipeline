import dlt
from pyspark.sql.functions import *

@dlt.view(
    name ='vw_products_silver'
)

def vw_products_silver():
    df_prod = spark.readStream.table("products_bronze")
    df_prod =df_prod.withColumn("Insert_ts",current_timestamp())
    return df_prod


dlt.create_streaming_table(
    name="products_silver"
)

dlt.create_auto_cdc_flow(
    target = "products_silver",
    source = "vw_products_silver",
    keys = ["product_id"],
    sequence_by = col("Insert_ts"),
    stored_as_scd_type = 1

)