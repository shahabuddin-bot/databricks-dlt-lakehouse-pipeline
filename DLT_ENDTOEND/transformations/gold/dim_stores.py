import dlt
from pyspark.sql.functions import *

#creating streaming view on top of siver view(not table)

@dlt.view(name ='vw_stores_gold')

def vw_stores_gold():
    df_stores =spark.readStream.table("vw_stores_silver")
    return df_stores

#create streaming sales table

dlt.create_streaming_table(
    name ="dim_stores_gold"
)

dlt.create_auto_cdc_flow(
    target = "dim_stores_gold",
    source = "vw_stores_gold",
    keys = ["store_id"],
    sequence_by = col("Insert_ts"),
    stored_as_scd_type = 2,
    except_column_list = ["Insert_ts"]
)
