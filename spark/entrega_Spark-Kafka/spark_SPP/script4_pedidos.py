#Nome:Samuel Pedrosa Pedrosa
#Importamos as librerias que imos empregar
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pandas as pd


appName = "Visor de pedidos global"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "pedidos"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Fixamos o nivel de rexistro a ERROR
spark.sparkContext.setLogLevel("ERROR")

# Creamos o steam de lectura
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

#Definimos a estrutura/esquema que vai ter o JSON
schema = StructType(
    [
        StructField('Tenda', StringType(), True),
        StructField('Categoria', StringType(), True),
        StructField('produto', StringType(), True),
	StructField('cantidade', FloatType(), True)
    ]
)

#Creamos un DF do que sacaremos todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))
#Realizamos a consulta para mostrar por pantalla os novos aticulos cada 5 segundos
query = df2 \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

#Intentamos escribir os elementos do topic a arquivo (Pero non funciona)
kafka_df = df2.selectExpr('cast(id as string) as key', 'to_json(struct(*)) as value')
kafka_df.show(truncate=False)
kafka_df.selectExpr("CAST value as STRING") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("pedidos", TOPIC) \
    .save("/home/hduser/Descargas/Avaliación_UD3/kafka.txt")
