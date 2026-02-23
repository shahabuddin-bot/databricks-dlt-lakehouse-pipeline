import dlt
from pyspark.sql.functions import *

@dlt.view(
    name ='vw_stores_silver'
) 
def vw_stores_silver():
    df_store =spark.readStream.table('stores_bronze')
    df_store = df_store.withColumn("store_name",regexp_replace(col("store_name"),"_",""))
    df_store =df_store.withColumn("Insert_ts",current_timestamp())
    return df_store


dlt.create_streaming_table(name="stores_silver")


dlt.create_auto_cdc_flow(
    target = "stores_silver",
    source = "vw_stores_silver",
    keys = ["store_id"],
    sequence_by = col("Insert_ts"),
    stored_as_scd_type = 1

)