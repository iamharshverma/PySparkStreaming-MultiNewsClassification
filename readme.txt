Steps to Execute the Project :

1) Simply type the following command on your command prompt to run the stream_producer script:

python3 stream_producer.py API-key fromDate toDate

Ex: python3 stream_producer.py 405cb3e5-b364-4df8-9f4a-905210534c1d 2019-01-3 2019-03-24

2) Start Zookeper and Kafka :
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties

3) Start the Consumer for kafka producer :
Run: python3 stream_consumer.py to consume the kafka topic data and save to news_data.csv file

4) Train and Test the Classifier Logistic and Naive Bayes on the consumed data with pipilene to perform data analysis and pre-processing on set of 30000 rows of news articles.
Run: python3 StreamingNewsClassification.py

-> This will Train both the classifiers and save the model in output folder along with pipeline

5) Create Kafka Direct Stream Topic "guardian2stream":

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic guardian2stream

6) Get the Spark Streaming jar from :
https://search.maven.org/artifact/org.apache.spark/spark-streaming-kafka-0-8-assembly_2.11/2.4.1/jar
Search : org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.1 and Download


7) Start and Run the kafka Batch Streaming to producer stream data on newly created Topic "guardian2stream":

Run: python3 stream_producer_batch.py

8) Start the Streaming Data Classifier in spark using below command:

spark-submit --jars /Users/harshverma/Downloads/spark-streaming-kafka-0-8-assembly_2.11-2.4.1.jar ~/PycharmProjects/SparkStreamNewsDataClassification/src/StreamingNewsClassification.py


9) Check Batch Streaming Classification output, Multiclassification Metrics, Performance of both classifers in console