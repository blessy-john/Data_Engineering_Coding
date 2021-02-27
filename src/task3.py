# Databricks notebook source
# The Iris dataset is downloaded from the UCI repositry into the local system 
# using the command curl -L "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data" -o /tmp/iris.csv
# The data is then loaded into the dbfs

import pyspark
from pyspark.sql.functions import *
from pyspark.ml.classification import *
from pyspark.ml.evaluation import *
from pyspark.ml.feature import *


myResultDir = "FileStore/tables/my_result/out_3_2"

 # 1. We define the Schema of our DF.
my_schema = pyspark.sql.types.StructType(
    [pyspark.sql.types.StructField("sepal_length", pyspark.sql.types.DecimalType(scale=1), True),
     pyspark.sql.types.StructField("sepal_width", pyspark.sql.types.DecimalType(scale=1), True),
     pyspark.sql.types.StructField("petal_length", pyspark.sql.types.DecimalType(scale=1), True),
     pyspark.sql.types.StructField("petal_width", pyspark.sql.types.DecimalType(scale=1), True),
     pyspark.sql.types.StructField("class", pyspark.sql.types.StringType(), True),

    ])

# 2. Operation C1: We create the DataFrame from the dataset and the schema
inputDF = spark.read.format("csv") \
                    .option("delimiter", ",") \
                    .option("quote", "") \
                    .option("header", "false") \
                    .schema(my_schema) \
                    .load("/FileStore/tables/my_data/task_3/iris.csv")

# The name of the columns are extracted
columns=inputDF.columns[:-1]
grouper = pyspark.ml.feature.VectorAssembler(inputCols=columns, outputCol='features')
data = grouper.transform(inputDF)

# convert text labels into indices
data = data.select(['features', 'class'])
label_indexer = pyspark.ml.feature.StringIndexer(inputCol='class', outputCol='label').fit(data)
data = label_indexer.transform(data)


#The data is then split to test data and the train data

train, test = data.randomSplit([0.70, 0.30])

# The model is created
lr = pyspark.ml.classification.LogisticRegression()

# The data is fitted into the model
model = lr.fit(train)



# predict on the test set
prediction = model.transform(test)




# evaluate the accuracy of the model using the test set
evaluator = pyspark.ml.evaluation.MulticlassClassificationEvaluator(metricName='accuracy')
accuracy = evaluator.evaluate(prediction)

# You should be able to run your model against the pred_data data frame

# ******************************************************************************************************************************

# Predicting Test Data  

pred_data = spark.createDataFrame(
 [(5.1, 3.5, 1.4, 0.2), 
 (6.2, 3.4, 5.4, 2.3)],
 ["sepal_length", "sepal_width", "petal_length", "petal_width"])

pred_data = grouper.transform(pred_data)
prediction=model.transform(pred_data)
prediction = prediction.select('features','prediction')
prediction.show()
prediction.rdd.saveAsTextFile(myResultDir)

# ********************************************************************************************************************************


print()
print('#####################################')
print("Accuracy is {}".format(accuracy))
print('#####################################')
print()

