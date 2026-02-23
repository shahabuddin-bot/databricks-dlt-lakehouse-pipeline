import dlt
from pyspark.sql.functions import *

#creating streaming view on top of siver view(not table)

@dlt.view(name ='vw_products_gold')

def vw_products_gold():
    df_prod =spark.readStream.table("vw_products_silver")
    return df_prod

#create streaming sales table

dlt.create_streaming_table(
    name ="dim_products_gold"
)

dlt.create_auto_cdc_flow(
    target = "dim_products_gold",
    source = "vw_products_gold",
    keys = ["product_id"],
    sequence_by = col("Insert_ts"),
    stored_as_scd_type = 2,
    except_column_list = ["Insert_ts"]
)
