#Nome:Samuel Pedrosa Pedrosa
#Importamos as librerias que imos empregar
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

appName = "Visor de Beneficios por categoria"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "vendas"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Fixamos o nivel de rexistro a ERROR
spark.sparkContext.setLogLevel("ERROR")

# Creamos o stream de lectura
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
	StructField('prezo', FloatType(), True)
    ]
)

#Creamos un DF do que sacaremos todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))
#Creamos a táboa de SQL sobre a que imos traballar
df2.createOrReplaceTempView('ventas2')
#Creamos a consulta que imos empregar
consulta = spark.sql("SELECT Categoria,sum(prezo) as Beneficios from ventas1 GROUP BY Categoria")
query = consulta \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()
query.awaitTermination()
