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

# Casteo a string os valores das columnas key->key_str e value->value_str
# A continuación elimínanse as columnas orixinais (.drop()) 
df = df.withColumn('key_str', df['key'].cast('string').alias('key_str')).drop(
    'key').withColumn('value_str', df['value'].cast('string').alias('key_str')).drop('value')

df.show()
