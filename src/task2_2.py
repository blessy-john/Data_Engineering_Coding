# Databricks notebook source
df=spark.read.parquet("/FileStore/tables/my_data/task_2")


# COMMAND ----------

#Importing the required modules

from pyspark.sql.functions import min,max,count,lit,col,sum
myResultDir = "FileStore/tables/my_result/out_2_2"

task_2=df.withColumn('min_price',lit(df.agg(min('price')).first().asDict()['min(price)'])).withColumn('max_price',lit(df.agg(max('price')).first().asDict()['max(price)'])).\
    withColumn('row_count',lit(df.agg(count('price')).first().asDict()['count(price)'])).select('min_price','max_price','row_count').distinct()

task_2.show()

task_2.write.format("csv").option("header", "true").mode("overwrite").save(myResultDir)


# COMMAND ----------


