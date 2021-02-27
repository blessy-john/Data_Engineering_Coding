# Databricks notebook source
# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------

import pyspark
import pyspark.dbutils

def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(",")
    return params

  
if __name__ == '__main__':
 

    # 3. We set the path to my_dataset and my_result
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/groceries.csv"
    myResultDir="FileStore/tables/my_result/out_1_3"
    my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    
    inputRDD=sc.textFile(my_dataset_dir)
    inputRDD=inputRDD.map(lambda x:process_line(x))
    
    # The first five data is taken
    task3 = inputRDD.flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:-x[1])
    print(task3.take(5))

    
    # The Entire data is written to out_1_3 and loaded from dbfs file system to local file system
    task3.saveAsTextFile(myResultDir)
    
