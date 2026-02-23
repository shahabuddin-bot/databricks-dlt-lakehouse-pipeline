import dlt
from pyspark.sql.functions import *

#create streaming sales table
@dlt.table(name ='sales_bronze')
def sales_bronze():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")\
            .load("/Volumes/md_shahabuddin_catalog/bronze/bronze_volume/sales/")
    return df
    
  #create streaming stores table
@dlt.table(name ='stores_bronze')
def stores_bronze():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")\
            .load("/Volumes/md_shahabuddin_catalog/bronze/bronze_volume/stores/")
    return df



#create streaming sales table
@dlt.table(name ='products_bronze')
def products_bronze():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")\
            .load("/Volumes/md_shahabuddin_catalog/bronze/bronze_volume/products/")
    return df


#create streaming sales table
@dlt.table(name ='customers_bronze')
def customers_bronze():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")\
            .load("/Volumes/md_shahabuddin_catalog/bronze/bronze_volume/customers/")
    return df
  