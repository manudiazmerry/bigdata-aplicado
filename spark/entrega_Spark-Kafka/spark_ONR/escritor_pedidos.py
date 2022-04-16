#escribir os pedidos a ficheiros
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

appName = "Streaming de escritura"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "pedidos"
#cambiamos o topic a pedios
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
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

df2.createOrReplaceTempView('pedidos')
consulta = spark.sql('SELECT * from pedidos')


# opción truncate = false para poder ver as datas e horas
query = consulta\
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option('header',True)\
    .option('path',"file:///home/hduser/code/spark/output/resultado.csv") \
    .option("checkpointLocation", "file:///home/hduser/code/spark/output/checkpoint/filesink_checkpoint") \
    .start()
#poñemos a ruta do ficheiro a escribir
query.awaitTermination()
