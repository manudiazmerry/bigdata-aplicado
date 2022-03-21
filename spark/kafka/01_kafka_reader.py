from pyspark.sql import SparkSession

appName = "Lectura Simple Kafka"
master = "local"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

kafka_servers = "localhost:9092"

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "exemplo") \
    .load()

df.show()
