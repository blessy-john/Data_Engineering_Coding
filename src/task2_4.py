# Databricks notebook source
from pyspark.sql.functions import min,max,count,lit,col,sum

df=spark.read.parquet("/FileStore/tables/my_data/task_2")

myResultDir = "FileStore/tables/my_result/out_2_4"

task4=df.filter(df['price']==df.agg(min('price')).first().asDict()['min(price)']).\
              filter(df['review_scores_value']==10).agg(sum('accommodates')).\
              withColumnRenamed('sum(accommodates)','People_Accommodated')
print(task4.show())

# The Entire data is written to out_2_4 and loaded from dbfs file system to local file system
task4.write.format("csv").option("header", "true").mode("overwrite").save(myResultDir)


# COMMAND ----------


