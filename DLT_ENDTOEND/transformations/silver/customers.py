import dlt
from pyspark.sql.functions import *

@dlt.view(
    name ='vw_customers_silver'
) 
def vw_customers_silver():
    df=spark.readStream.table('customers_bronze')
    df=df.withColumn("name",upper(col("name")))
    df=df.withColumn("domain",split(col("email"),"@")[1])
    df =df.withColumn("Insert_ts",current_timestamp())
    return df



dlt.create_streaming_table(name="customers_silver")


dlt.create_auto_cdc_flow(
    target = "customers_silver",
    source = "vw_customers_silver",
    keys = ["customer_id"],
    sequence_by = col("Insert_ts"),
    stored_as_scd_type = 1

)