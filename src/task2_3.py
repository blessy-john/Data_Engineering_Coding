# Databricks notebook source
df=spark.read.parquet("/FileStore/tables/my_data/task_2")

myResultDir="FileStore/tables/my_result/out_2_3"

task3=df.filter(df['price']>5000).filter(df['review_scores_value']==10).\
                    groupBy('property_type').avg('bathrooms','bedrooms').\
                    withColumnRenamed('avg(bathrooms)',' avg_bathrooms').\
                    withColumnRenamed('avg(bedrooms)',' avg_bedrooms')
task3.show()

# The Entire data is written to out_2_3 and loaded from dbfs file system to local file system
task3.write.format("csv").option("header", "true").mode("overwrite").save(myResultDir)

