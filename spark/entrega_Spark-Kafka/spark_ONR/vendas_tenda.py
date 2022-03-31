#visor de vendas por tenda
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

appName = "Streaming de vendas por tenda"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "vendas"
#poñemos o topic adecuado
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
	StructField('precio', FloatType(),True),
	StructField('categoria', StringType(), True)
    ]
)
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

df2.createOrReplaceTempView('vendas')
consulta = spark.sql('SELECT tienda, SUM(precio) as beneficio, COUNT(*) FROM vendas GROUP BY tienda')

# aquery e un sumatorio do precio por tenda  segundo o numero de veces que parece 
# opción truncate = false para poder ver as datas e horas
query = consulta\
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
