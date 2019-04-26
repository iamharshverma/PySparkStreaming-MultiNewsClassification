from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.ml.classification import NaiveBayesModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import lit
from pyspark.ml.classification import LogisticRegressionModel

# Code for News Classification on Streaming Data
root = "/Users/harshverma/PycharmProjects/SparkStreamNewsDataClassification"
output_folder_path  = root + "/Output/"
input_folder_path = root + "/data/"

# News Categories
news_categories = ['Australia news', 'US news', 'Football', 'World news' , 'Sport' , 'Television & radio' , 'Environment', 'Science' , 'Media'
               'News',  'Opinion' , 'Politics', 'Business', 'UK news', 'Society', 'Life and style', 'Inequality', 'Art and design', 'Books', 'Stage'
                'Film','Music', 'Global', 'Food', 'Culture', 'Community', 'Money', 'Technology', 'Travel',  'From the Observer', 'Fashion', 'Crosswords', 'Law']

# -- Functions --

# News Category Label Mapper with Index
def mapSpeciesTypeWithNumericLabel(news_category):
    try:
        news_category = str(news_category)
        news_category = news_category.split("=")[1].strip().replace(")", "")
        news_category = int(float(str(news_category)))
        # Prediction
        news_category = news_category + 1

    except:
        print(news_category)
        print(type(news_category))

    if news_category == 0:
        return 'Australia news'
    elif news_category == 1:
        return 'US news'
    elif news_category == 2:
        return 'Football'
    elif news_category == 3:
        return 'World news'
    elif news_category == 4:
        return 'Sport'
    elif news_category == 5:
        return 'Television & radio'
    elif news_category == 6:
        return 'Environment'
    elif news_category == 7:
        return 'Science'
    elif news_category == 8:
        return 'Media'
    elif news_category == 9:
        return 'News'
    elif news_category == 10:
        return 'Opinion'
    elif news_category == 11:
        return 'Politics'
    elif news_category == 12:
        return 'Business'
    elif news_category == 13:
        return 'UK news'
    elif news_category == 14:
        return 'Society'
    elif news_category == 15:
        return 'Life and style'
    elif news_category == 16:
        return 'Inequality'
    elif news_category == 17:
        return 'Art and design'
    elif news_category == 18:
        return 'Books'
    elif news_category == 19:
        return 'Stage'
    elif news_category == 20:
        return 'Film'
    elif news_category == 21:
        return 'Music'
    elif news_category == 22:
        return 'Global'
    elif news_category == 23:
        return 'Food'
    elif news_category == 24:
        return 'Culture'
    elif news_category == 25:
        return 'Community'
    elif news_category == 26:
        return 'Money'
    elif news_category == 27:
        return 'Technology'
    elif news_category == 28:
        return 'Travel'
    elif news_category == 29:
        return 'From the Observer'
    elif news_category == 30:
        return 'Fashion'
    elif news_category == 31:
        return 'Crosswords'
    elif news_category == 32:
        return 'Law'
    else:
        return None


def createLabeledPoints(field):
    species = mapSpeciesTypeWithNumericLabel(field)
    return species


def process(time, rdd):
    print("=========*********************************************** %s ***********************************************============" % str(time))
    print("\n")
    if not (rdd.isEmpty()):
        df = spark.createDataFrame(rdd, ["label", "text"])
        print("=========***********************************************$ Raw Data From Stream $***********************************************=========")
        df.show()
        pipeline_data = loded_pipeline.transform(df)
        print("\n")
        print("=========***********************************************$ Transformed Data After Running Pre Loded Pipeline $***********************************************=========")
        pipeline_data.show()
        print("\n\n")

        print("=========***********************************************$ Classification Using Pre Trained Logistic Classification Model $***********************************************=========")
        predictions = saved_logistic_model.transform(pipeline_data)

        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
        f1 = evaluator.setMetricName("f1").evaluate(predictions)
        weightedPrecision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
        weightedRecall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
        accuracyNaiveBayes = evaluator.setMetricName("accuracy").evaluate(predictions)

        predictions = predictions.select("label", "prediction")

        predictions = predictions.withColumn("Current Stream Accuracy %", lit(str((accuracyNaiveBayes) * 100) + "%"))
        predictions = predictions.withColumn("Current Stream Error %", lit(str((1.0 - accuracyNaiveBayes) * 100) + "%"))
        predictions = predictions.withColumn("Current Stream F1 Score",  lit(str(f1)))
        predictions = predictions.withColumn("Current Stream weightedRecall", lit(str(weightedRecall)))
        predictions = predictions.withColumn("Current Stream weightedPrecision", lit(str(weightedPrecision)))

        # To Print Data Frame Schema for Debbuging
        #predictions.printSchema()

        label = mapSpeciesTypeWithNumericLabel(predictions.select("prediction").first())
        labelInitial = mapSpeciesTypeWithNumericLabel(predictions.select("label").first())
        global total_count_logistic_classification
        global correct_count_logistic_classification
        total_count_logistic_classification = total_count_logistic_classification + 1
        if(labelInitial == label):
            correct_count_logistic_classification = correct_count_logistic_classification + 1


        overall_accuracy_percent = (float(correct_count_logistic_classification) / float(total_count_logistic_classification)) * 100
        predictions = predictions.withColumn("News_Category_Predicted", lit(str(label)))
        predictions = predictions.withColumn("News_Category_InitalLabel", lit(str(labelInitial)))


        predictions.show()

        # Overall Stats
        total_predictions = predictions.select("label")
        total_predictions = total_predictions.withColumn("Overall Correct Count", lit(str(correct_count_logistic_classification)))
        total_predictions = total_predictions.select("Overall Correct Count")
        total_predictions = total_predictions.withColumn("Total Count", lit(str(total_count_logistic_classification)))
        total_predictions = total_predictions.withColumn("Overall Accuracy Percent(%)", lit(str(overall_accuracy_percent) + "%"))
        total_predictions = total_predictions.withColumn("Overall Error Percent(%)", lit(str(100-overall_accuracy_percent) + "%"))

        print("\n")
        print("=========***********************************************$ Overall Classification Metrics Logistic Classification Model $***********************************************=========")
        total_predictions.show()

        # print("Test Error for Naive Bayes :" + str((1.0 - accuracyNaiveBayes) * 100) + "%")
        # print("Test Accuracy for Naive Bayes :" + str((accuracyNaiveBayes) * 100) + "%")
        # print("Test weightedRecall for Naive Bayes :" + str(weightedRecall))
        # print("Test weightedPrecision for Naive Bayes :" + str(weightedPrecision))
        # print("Test f1 score for Naive Bayes :" + str(f1))

        # Naive bayes Model Classification

        print("\n\n")
        print("=========***********************************************$ Classification Using Pre Trained Naive Bayes Classification Model $***********************************************=========")
        print("\n")
        naive_bayes_predictions = saved_naive_bayes_model.transform(pipeline_data)

        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
        f1 = evaluator.setMetricName("f1").evaluate(naive_bayes_predictions)
        weightedPrecision = evaluator.setMetricName("weightedPrecision").evaluate(naive_bayes_predictions)
        weightedRecall = evaluator.setMetricName("weightedRecall").evaluate(naive_bayes_predictions)
        accuracyNaiveBayes = evaluator.setMetricName("accuracy").evaluate(naive_bayes_predictions)

        naive_bayes_predictions = naive_bayes_predictions.select("label", "prediction")

        naive_bayes_predictions = naive_bayes_predictions.withColumn("Current Stream Accuracy %", lit(str((accuracyNaiveBayes) * 100) + "%"))
        naive_bayes_predictions = naive_bayes_predictions.withColumn("Current Stream Error %", lit(str((1.0 - accuracyNaiveBayes) * 100) + "%"))
        naive_bayes_predictions = naive_bayes_predictions.withColumn("Current Stream F1 Score", lit(str(f1)))
        naive_bayes_predictions = naive_bayes_predictions.withColumn("Current Stream weightedRecall", lit(str(weightedRecall)))
        naive_bayes_predictions = naive_bayes_predictions.withColumn("Current Stream weightedPrecision", lit(str(weightedPrecision)))

        # To Print Data Frame Schema for Debbuging
        # predictions.printSchema()

        label_naive_bayes = mapSpeciesTypeWithNumericLabel(naive_bayes_predictions.select("prediction").first())
        labelInitial_naive_bayes = mapSpeciesTypeWithNumericLabel(naive_bayes_predictions.select("label").first())

        # Loading Global Variables
        global total_count_naive_bayes_classification
        global correct_count_naive_bayes_classification

        total_count_naive_bayes_classification = total_count_naive_bayes_classification + 1
        if (label_naive_bayes == labelInitial_naive_bayes):
            correct_count_naive_bayes_classification = correct_count_naive_bayes_classification + 1

        overall_accuracy_naive_bayes_percent = (float(correct_count_naive_bayes_classification) / float(total_count_naive_bayes_classification)) * 100
        naive_bayes_predictions = naive_bayes_predictions.withColumn("News_Category_Predicted", lit(str(label_naive_bayes)))
        naive_bayes_predictions = naive_bayes_predictions.withColumn("News_Category_InitalLabel", lit(str(labelInitial_naive_bayes)))

        naive_bayes_predictions.show()
        print("\n")

        # Overall Stats
        total_naive_bayes_predictions = naive_bayes_predictions.select("label")
        total_naive_bayes_predictions = total_naive_bayes_predictions.withColumn("Overall Correct Count", lit(str(correct_count_naive_bayes_classification)))
        total_naive_bayes_predictions = total_naive_bayes_predictions.select("Overall Correct Count")
        total_naive_bayes_predictions = total_naive_bayes_predictions.withColumn("Total Count", lit(str(total_count_naive_bayes_classification)))
        total_naive_bayes_predictions = total_naive_bayes_predictions.withColumn("Overall Accuracy Percent(%)",
                                                         lit(str(overall_accuracy_naive_bayes_percent) + "%"))
        total_naive_bayes_predictions = total_naive_bayes_predictions.withColumn("Overall Error Percent(%)",
                                                         lit(str(100 - overall_accuracy_naive_bayes_percent) + "%"))

        print("=========***********************************************$ Overall Classification Metrics Naive Bayes Classification Model $***********************************************=========")

        total_naive_bayes_predictions.show()

        print("\n")
        print("=========*********************************************** End of Single Stream ***********************************************=========")


# Main Method
if __name__=="__main__":

    # Initialize global count variables for both classification Model
    # Logistic Classification count variables
    correct_count_logistic_classification = 0
    total_count_logistic_classification = 0

    # Naive Bayes Classification count variables
    correct_count_naive_bayes_classification = 0
    total_count_naive_bayes_classification = 0

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.getOrCreate()
    ssc = StreamingContext(sc, 1)
    # Setting model path
    save_pipeline_path = output_folder_path + "pipeline"
    saved_logistic_model_path = output_folder_path + "LogisticClassificationModel"
    saved_naive_bayes_model_path = output_folder_path + "NaiveBayesClassificationModel"

    # Loading Pipeline and Model
    loded_pipeline = PipelineModel.load(save_pipeline_path)
    saved_logistic_model = LogisticRegressionModel.load(saved_logistic_model_path)
    saved_naive_bayes_model = NaiveBayesModel.load(saved_naive_bayes_model_path)

    # Creating Kafka Stream
    kvs = KafkaUtils.createDirectStream(
        ssc, topics=['guardian2stream'], kafkaParams={"metadata.broker.list": 'localhost:9092'})
    document_tuple = kvs.map(lambda line: (int(line[1].split("||")[0].strip().encode("ascii", "ignore")), line[1].split("||")[1].encode("ascii", "ignore")))
    document_tuple.pprint()
    document_tuple.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopGraceFully=True)