from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

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


# Divide as liñas en palabras
words = df.select(explode(split(df.value, " ")).alias("word"))

# Xenera a conta de palabras
wordCounts = words.groupBy("word").count()

# Stream de escritura
# opción truncate = false para poder ver as datas e horas
query = wordCounts \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("complete") \
    .start()

query.awaitTermination()
