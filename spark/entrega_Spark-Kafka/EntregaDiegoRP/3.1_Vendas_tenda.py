from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

#Topic Vendas
appName = "3.1_vendas_tenda"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "vendas"

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

#Estructura Venda
schema = StructType(
    [
    	StructField('nombreTienda', StringType(), True),
        StructField('categoria', StringType(), True),
        StructField('producto', StringType(), True),
        StructField('precio', StringType(), True)
    ]
)

df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

#Consulta SQL
df2.createOrReplaceTempView('vendas1')
df_sql = spark.sql('SELECT nombreTienda, COUNT(nombreTienda), SUM(precio) FROM vendas1 GROUP BY nombreTienda')

query = df_sql \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("complete") \
    .trigger(processingTime='2 seconds') \
    .start()
  
    	

query.awaitTermination()
