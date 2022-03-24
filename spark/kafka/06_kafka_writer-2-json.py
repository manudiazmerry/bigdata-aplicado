from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

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


columns = ["id","nome","apelido"]
data = [("10", "pepe","papun"), ("20", "maria","marinha"),("30","manuel","manuelon")]
df = spark.createDataFrame(data).toDF(*columns)

df.show()

kafka_df = df.selectExpr('cast(id as string) as key', 'to_json(struct(*)) as value')

kafka_df.show(truncate=False)

# Write key-value data from a DataFrame to a specific Kafka topic specified in an option
kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("topic", TOPIC) \
    .save()
