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
    myResultDir_a="FileStore/tables/my_result/out_1_2_a"
    myResultDir_b="FileStore/tables/my_result/out_1_2_b"
    my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    
    inputRDD=sc.textFile(my_dataset_dir)
    inputRDD=inputRDD.map(lambda x:process_line(x))
    
    
    # 3. Operation P1: We persist wordsRDD, as we are going to use it more than once.
    # Please remember each action using an RDD will indeed trigger its lazy recomputation.
    # This might sound counter intuitive, but it makes perfect sense in the context of big data.
    # The idea is that we want to optimise the use of our resources.
    # Thus, if an action A requires an RDD to be computed, it is computed, used for the action A, but right after it discarded,
    # as we want to leverage as much as possible the memory of our nodes in the cluster.

    # Thus, if two actions A1 and A2 require the RDDi, it will be indeed recomputed twice, in this case leading to a waste of time.
    # To get the best tradeoff between memory usage and performance, we use the operation persist() to keep in memory any RDD that is
    # going to be used in more than 1 action. Thus, it will be computed just once.

    # In any case, persist can story the RDD in memory or in disk (or in a combination of both).
    # If it stores the RDD in memory, it does it by storing it in the heap area of the JVM of the executor process of each node.

    #         C1: parallelize             T1: flatMap            P1: persist    ------------
    # dataset -----------------> inputRDD ------------> wordsRDD -------------> | wordsRDD |
    #                                                                           ------------

    
    wordRDD=inputRDD.flatMap(lambda x:x)
    
    # The Entire data is written to out_1_2_a and loaded from dbfs file system to local file system
    
    wordRDD.saveAsTextFile(myResultDir_a)
    wordRDD.persist()

    # 4. Operation A1: We count how many items are in the collection wordsRDD, to ensure there are 8 and not 2.

    #         C1: parallelize             T1: flatMap            P1: persist    ------------
    # dataset -----------------> inputRDD ------------> wordsRDD -------------> | wordsRDD |
    #                                                                           ------------
    #                                                                           |
    #                                                                           | A1: count
    #                                                                           |-----------> res1VAL
    #
    # Unique words are extracted and counted
    
    word_count=wordRDD.distinct().count()
    
    # The Entire data is written to out_1_2_b and loaded from dbfs file system to local file system
    
    dbutils.fs.put("FileStore/tables/my_result/out_1_2_b",'Count: \n {} '.format(word_count))
        
    
    
    
    
