import dlt
from pyspark.sql.functions import *

@dlt.view(
    name = 'vw_sales_silver'
)

def vw_sales_silver():
    df_sales = spark.readStream.table('sales_bronze')
    df_sales = df_sales.withColumn("pricePerSale", round(col("total_amount")/col("quantity"),2))
    df_sales =df_sales.withColumn("Insert_ts",current_timestamp())
    return df_sales

# create streaming_sales_silver_table

dlt.create_streaming_table(
    name = "sales_silver"
)

dlt.create_auto_cdc_flow(

    target = "sales_silver",
    source = "vw_sales_silver",
    keys = ["sales_id"],
    sequence_by = col("Insert_ts"),
    stored_as_scd_type = 1
)

