from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("TrackingStream").getOrCreate()

dataStreamWriter = (spark.readStream
                    .format("mongodb")
                    .option("spark.mongodb.connection.uri", "mongodb://root:root@mongodb.mongodb.svc.cluster.local:27017")
                    .option('spark.mongodb.database', "Bocato")
                    .option('spark.mongodb.collection', "NotificationTracking")
                    .option('spark.mongodb.read.readPreference.name', "primaryPreferred")
                    .option("spark.mongodb.change.stream.publish.full.document.only", "true")
                    # .schema(readSchema)
                    .load()
                    # manipulate your streaming data
                    .writeStream
                    .format("console")
                    .trigger(continuous="5 second")
                    .outputMode("complete")
                    )

# run the query
query = dataStreamWriter.start()
query.awaitTermination()
