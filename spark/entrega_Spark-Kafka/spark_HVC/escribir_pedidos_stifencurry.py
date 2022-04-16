#Importo librerías
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pandas as pd

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

# Stream de lectura, leo el tópico de pedidos sin ser en streaming, los pedidos que ya haya
df = spark \
    .read \
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

#Convierto el dataframe con la estructura que cree antes
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

#Paso el PYSpark dataframe a pandas para poder pasarlo a csv fácilmente
pandasDF = df2.toPandas()
pandasDF.to_csv('pedidos.csv', encoding='utf-8')
