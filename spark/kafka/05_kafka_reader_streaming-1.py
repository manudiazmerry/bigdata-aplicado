from pyspark.sql import SparkSession

appName = "Streaming de Lectura Kafka"
master = "local"
KAFKA_SERVERS = "localhost:9092"
TOPIC = "palabras"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Fixar o nivel de rexistro/log a ERROR
spark.sparkContext.setLogLevel("ERROR")

# Stream de lectura
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .load()

# Stream de escritura
# opción truncate = false para poder ver as datas e horas
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp") \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
