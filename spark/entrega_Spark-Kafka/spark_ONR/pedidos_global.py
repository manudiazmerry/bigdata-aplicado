#visor de pedidos global
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

appName = "Streaming de pedidos global"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "pedidos"
#cambiamos o topic a pedidos
spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Fixar o nivel de rexistro/log a ERROR
spark.sparkContext.setLogLevel("ERROR")

# Stream de lectura
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .load()
df = df.selectExpr("CAST(value AS STRING)")
# Stream de escritura
schema = StructType(
    [
        StructField('tienda', StringType(), True),
	StructField('producto', StringType(), True),
	StructField('cantidade', StringType(),True),
	StructField('categoria', StringType(), True)
    ]
)
#cantidade ten que String se non danos null  e xa se castea na query
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

df2.createOrReplaceTempView('pedidos')
consulta = spark.sql('SELECT * from pedidos')
#visualizamos a tabla cople

# opción truncate = false para poder ver as datas e horas
query = consulta\
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='40 seconds')\
    .start()

query.awaitTermination()
#outputMode podemolo a append para que se actualize de cada paso o tigger e para que no lo mostre a intervalos de 40 segundos 
