from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

#Topic Pedidos
appName = "4_escribirPedidos"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "pedidos"

spark = SparkSession.builder \
	.master(master) \
	.appName(appName) \
	.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark \
	.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", KAFKA_SERVERS) \
	.option("subscribe", TOPIC) \
	.load()
	
df = df.selectExpr("CAST(value AS STRING)")


#Estructura Pedidos
schema = StructType(
    [
    	StructField('nombreTienda', StringType(), True),
        StructField('categoria', StringType(), True),
        StructField('producto', StringType(), True),
        StructField('cantidad', StringType(), True)
    ]
)

df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

query = df2 \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') \
    .start()
    
# Leemos nuestro df2 y lo almacenamos como json
# Para crear el nº mínimo de archivos, agrupamos las entradas recividas cada 10s
df2.writeStream \
	.format("json") \
	.option("path", "/home/hduser/bigdata-aplicado/spark/kafka/") \
    	.option("checkpointLocation", "/home/hduser/bigdata-aplicado/spark/kafka") \
    	.outputMode("append") \
    	.trigger(processingTime='10 seconds') \
    	.start()
    	

query.awaitTermination()
