from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

#Topic Pedidos
appName = "3.4_visor_vendas_global"
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

#Consulta SQL
df2.createOrReplaceTempView('pedidos1')
df_sql = spark.sql('SELECT nombreTienda, categoria, producto, SUM(cantidad) FROM pedidos1 GROUP BY nombreTienda, categoria, producto ORDER BY nombreTienda, categoria, producto')

query = df_sql \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("complete") \
    .trigger(processingTime='2 seconds') \
    .start()
  
    	

query.awaitTermination()
