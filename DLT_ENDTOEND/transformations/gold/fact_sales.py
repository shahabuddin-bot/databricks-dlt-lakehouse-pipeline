import dlt
from pyspark.sql.functions import *

#creating streaming view on top of siver view(not table)

@dlt.view(name ='vw_sales_gold')

def vw_sales_gold():
    df_sales =spark.readStream.table("vw_sales_silver")
    return df_sales

#create streaming sales table

dlt.create_streaming_table(
    name ="fact_sales_gold"
)

dlt.create_auto_cdc_flow(
    target = "fact_sales_gold",
    source = "vw_sales_gold",
    keys = ["sales_id"],
    sequence_by = col("Insert_ts"),
    stored_as_scd_type = 1
)
