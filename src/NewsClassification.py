from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF , IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import NaiveBayes
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pathlib import Path
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# import comet_ml in the top of your file
from comet_ml import Experiment

# Add the following code anywhere in your machine learning file
experiment = Experiment(api_key="RI7Zo8mbnT7mIu8Z67Q1EtcL6",
                        project_name="mltest", workspace="harshverma59")

def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent

root = str(get_project_root())
input_data_path = root + "/data/"
output_folder_path  = root + "/Output/"

SparkContext.setSystemProperty('spark.executor.memory', '12g')
SparkContext.setSystemProperty('spark.default.parallelism', '100')
sc =SparkContext()

spark = SparkSession \
    .builder \
    .appName("basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark.conf.set("spark.executor.memory", "2g")

sqlContext = SQLContext(sc)
data = sqlContext.read.format('com.databricks.spark.csv').option("inferSchema", "true").load(input_data_path + 'news_data2.csv')
data.show(5)
data.printSchema()

data = (data.withColumnRenamed('_c0', 'index')
       .withColumnRenamed('_c1', 'heading')
       .withColumnRenamed('_c2', 'text'))

data.describe().show()


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), field1=str(fields[1]), field2=str(fields[2]))

countTokens = udf(lambda words: len(words), IntegerType())
data.groupBy(data[0]).count().orderBy(col("count").desc()).show()

# tokenizer

tokenizer = Tokenizer(inputCol= "text", outputCol="words")
tokenized = tokenizer.transform(data)
tokenized.select("text", "words").withColumn("tokens", countTokens(col("words"))).show(truncate=False)


remover = StopWordsRemover(inputCol="words", outputCol="filtered")


hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=3000)
#featurizedData = hashingTF.transform(tokenized)
# alternatively, CountVectorizer can also be used to get term frequency vectors

label_stringIdx = StringIndexer(inputCol = "index", outputCol = "label")

idf = IDF(inputCol="rawFeatures", outputCol="features")
#idfModel = idf.fit(featurizedData)
#rescaledData = idfModel.transform(featurizedData)


pipeline = Pipeline(stages=[tokenizer, remover, hashingTF , idf, label_stringIdx])

# Fit the pipeline to training documents.
pipelineFit = pipeline.fit(data)
dataset = pipelineFit.transform(data)
dataset.select("text", "features").show()


# Save the Pipeline
pipelineFit.save(output_folder_path + "pipeline")

dataset.show(20)


# set seed for reproducibility and Split Data in 80-20% for Train and Test Data Set
(trainingData, testData) = dataset.randomSplit([0.8, 0.2], seed = 100)
print("Training Dataset Count: " + str(trainingData.count()))
print("Test Dataset Count: " + str(testData.count()))


#Logistic Regression Classification
lr = LogisticRegression(maxIter=25, regParam=0.3, elasticNetParam=0)
lrModel = lr.fit(trainingData)
predictions = lrModel.transform(testData)
predictions.filter(predictions['prediction'] == 0).select("text","index","probability","label","prediction").orderBy("probability", ascending=False).show(n = 10, truncate = 30)



evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print("Test Error for Logistic Regression :" + str((1.0 - accuracy)*100)+ "%")
print("Test Accuracy for Logistic Regression :" + str((accuracy)*100)+ "%")

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction" ,metricName='f1')
f1 = evaluator.setMetricName("f1").evaluate(predictions)
weightedPrecision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
weightedRecall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)

print("Test weightedRecall for Logistic Regression :" + str(weightedRecall))
print("Test weightedPrecision for Logistic Regression :" + str(weightedPrecision))
print("Test f1 score for Logistic Regression :" + str(f1))


# Save model
save_model_path = output_folder_path + "LogisticClassificationModel"
lrModel.write().overwrite().save(save_model_path)

print("Logistic Classification Model Successfully trained and saved in project Output directory")

experiment.log_metric(name="accuracy_score_logistic", value=str((accuracy)*100))
experiment.log_metric(name="error_score_logistic", value=str(1.0 - accuracy)*100)

# Second Model Training and Classification
#Naive Bayes Classification


nb = NaiveBayes(smoothing=1)
model = nb.fit(trainingData)
predictions = model.transform(testData)
predictions.filter(predictions['prediction'] == 0).select("text", "index", "probability", "label", "prediction").orderBy("probability", ascending=False).show(n = 10, truncate = 30)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions)
accuracyNaiveBayes = evaluator.evaluate(predictions)

print("Test Error for Naive Bayes :" + str((1.0 - accuracyNaiveBayes)*100)+ "%")
print("Test Accuracy for Naive Bayes :" + str((accuracyNaiveBayes)*100)+ "%")

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
f1 = evaluator.setMetricName("f1").evaluate(predictions)
weightedPrecision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
weightedRecall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
accuracyNaiveBayes = evaluator.setMetricName("accuracy").evaluate(predictions)


print("Test weightedRecall for Naive Bayes :" + str(weightedRecall))
print("Test weightedPrecision for Naive Bayes :" + str(weightedPrecision))
print("Test f1 score for Naive Bayes :" + str(f1))


# Save model
save_model_path = output_folder_path + "NaiveBayesClassificationModel"
model.write().overwrite().save(save_model_path)

print("Naive Bayes Model Successfully trained and saved in project Output directory")

experiment.log_metric(name="accuracy_score_naive_bayes", value=str((accuracyNaiveBayes)*100))
experiment.log_metric(name="error_score_naive_bayes", value=str(1.0 - accuracyNaiveBayes)*100)
