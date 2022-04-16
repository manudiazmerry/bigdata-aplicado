# Visor de pedidos global.
# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Asignamos o nome do servidor, o tópico
appName = "Exercicio 4"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "pedidos"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# Pasamos o server, topic no df
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .load()
    
df = df.selectExpr("CAST(value AS STRING)")
# Creamos a estrutura do df
schema = StructType(
    [
       StructField('tenda', StringType(), True),
       StructField('categoria', StringType(), True),
       StructField('produto', StringType(), True),
       StructField('prezo', FloatType(),True)
    ])

df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

df2.createOrReplaceTempView('pedidos')
consulta = spark.sql('SELECT tenda, categoria, produto, prezo FROM pedidos')

query = consulta \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()

query.awaitTermination()
