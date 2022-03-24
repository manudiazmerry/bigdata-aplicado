from pyspark.sql import SparkSession

appName = "Escritura a Kafka"
master = "local"
KAFKA_SERVERS = "localhost:9092"
TOPIC = "nomes"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Fixar o nivel de rexistro/log a ERROR
spark.sparkContext.setLogLevel("ERROR")


columns = ["key","value"]
data = [("1", "pepe"), ("2", "maria"),("3","manuel")]
df = spark.createDataFrame(data).toDF(*columns)

#df.show()

# Write key-value data from a DataFrame to a specific Kafka topic specified in an option
df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("topic", TOPIC) \
    .save()


