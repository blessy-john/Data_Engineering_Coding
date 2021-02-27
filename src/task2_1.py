# Databricks notebook source
df=spark.read.parquet("/FileStore/tables/my_data/task_2")
df.show()
