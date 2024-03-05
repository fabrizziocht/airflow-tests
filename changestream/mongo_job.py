from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType
spark = SparkSession.builder.appName("TrackingStream").getOrCreate()


readSchema = StructType([
    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ]), True),
    StructField("specversion", StringType(), True),
    StructField("id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("type", StringType(), True),
    StructField("datacontenttype", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("triggerStatus", StringType(), True),
    StructField("data", StructType([
        StructField("eventDetails", StructType([
            StructField("eventType", StringType(), True),
            StructField("levelId", IntegerType(), True)
        ]), True),
        StructField("transactionDate", TimestampType(), True),
        StructField("customerId", StringType(), True),
        StructField("expirationDate", TimestampType(), True),
        StructField("bonusAmount", StructType([
            StructField("value", DoubleType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("requiredRollover", StructType([
            StructField("value", DoubleType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("campaignDetails", StructType([
            StructField("campaignId", IntegerType(), True),
            StructField("campaignPublicName", StringType(), True)
        ]), True),
        StructField("bonusDetails", StructType([
            StructField("bonusId", IntegerType(), True),
            StructField("bonusPublicName", StringType(), True),
            StructField("bonusCategory", StringType(), True),
            StructField("settings", StructType([
                StructField("productSettings", StructType([
                    StructField("sports", StructType([
                        StructField("allowed", StringType(), True)
                    ]), True)
                ]), True)
            ]), True)
        ]), True),
        StructField("depositDetails", StructType([
            StructField("paymentReference", StringType(), True)
        ]), True),
        StructField("tenantId", IntegerType(), True)
    ]), True)
])


dataStreamWriter = (spark.readStream
                    .format("mongodb")
                    .option("spark.mongodb.connection.uri", "mongodb://root:root@mongodb.mongodb.svc.cluster.local:27017")
                    .option('spark.mongodb.database', "Bocato")
                    .option('spark.mongodb.collection', "NotificationTracking")
                    .option('spark.mongodb.read.readPreference.name', "primaryPreferred")
                    # .option("change.stream.publish.full.document.only", "true")
                    .schema(readSchema)
                    .load()
                    # manipulate your streaming data
                    .writeStream
                    .format("console")
                    .trigger(continuous="1 second")
                    .outputMode("append")
                    )

# run the query
query = dataStreamWriter.start()
