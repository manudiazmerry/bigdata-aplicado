from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

appName = "Streaming de Lectura JSON Kafka"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "pedidos"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Fixar o nivel de rexistro/log a ERROR
spark.sparkContext.setLogLevel("ERROR")

# Stream de lectura do tópico de pedidos
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .load()


df = df.selectExpr("CAST(value AS STRING)")

# Defino o schema dos JSON codificado no campo value
schema = StructType(
    [
	StructField('tenda', StringType(), True),
        StructField('categoria', StringType(), True),
        StructField('producto', StringType(), True),
        StructField('cantidade', FloatType(), True)
    ]
)

#Cámbiolle a estructura ao dataframe pola creada anteriormente
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

#Creo a vista pedidos para realizar consultas sobre ela
df2.createOrReplaceTempView('pedidos')

#Realizo unha consulta na que saco todos os datos de pedidos
consulta = spark.sql("SELECT *  FROM pedidos")

# Stream de escritura
# Escribo a consulta por consola esta vez con outputmode en append para que vaia engadindo os datos conforme os vaia lendo
query = consulta \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .trigger(processingTime='3 seconds') \
    .start()

query.awaitTermination()
