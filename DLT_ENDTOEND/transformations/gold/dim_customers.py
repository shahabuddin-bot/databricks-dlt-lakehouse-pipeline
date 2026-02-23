import dlt
from pyspark.sql.functions import *

#creating streaming view on top of siver view(not table)

@dlt.view(name ='vw_customers_gold')

def vw_customers_gold():
    df_cus =spark.readStream.table("vw_customers_silver")
    return df_cus

#create streaming sales table

dlt.create_streaming_table(
    name ="dim_customers_gold"
)

dlt.create_auto_cdc_flow(
    target = "dim_customers_gold",
    source = "vw_customers_gold",
    keys = ["customer_id"],
    sequence_by = col("Insert_ts"),
    stored_as_scd_type = 2,
    except_column_list = ["Insert_ts"]
)
