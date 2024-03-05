from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.\
    appName("TrackingStream").\
    config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.1').\
    getOrCreate()

# define a streaming query
dataStreamWriter = (spark.readStream
                    .format("mongodb")
                    .option("spark.mongodb.connection.uri", "mongodb://root:root@mongodb.mongodb.svc.cluster.local:27017")
                    .option('spark.mongodb.database', "Bocato")
                    .option('spark.mongodb.collection', "NotificationTracking")
                    .option("change.stream.publish.full.document.only", "True")
                    # .schema(readSchema)
                    .load()
                    # manipulate your streaming data
                    .writeStream
                    .format("console")
                    .trigger(continuous="1 second")
                    .outputMode("append")
                    )

# run the query
query = dataStreamWriter.start()
