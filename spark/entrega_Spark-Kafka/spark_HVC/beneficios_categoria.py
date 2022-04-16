from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

appName = "Streaming de Lectura JSON Kafka"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "vendas"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Fixar o nivel de rexistro/log a ERROR
spark.sparkContext.setLogLevel("ERROR")

# Stream de lectura do topico vendas
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
        StructField('prezo', FloatType(), True)
    ]
)

#Pásolle a estructura ao dataframe
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

#Creo a vista vendas para realizar as consultas sobre ela
df2.createOrReplaceTempView('vendas')

#Realizo a consulta na que escollo a categoria, o sumatorio dos prezos e os ordeno por cateoria
consulta = spark.sql("SELECT categoria, SUM(prezo) AS beneficios FROM vendas GROUP BY categoria")

# Stream de escritura
# Escribo a consola a consulta cada 3 segundos, como ten consultas de agregación engadinlle o output mode 'complete'
query = consulta \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("complete") \
    .trigger(processingTime='3 seconds') \
    .start()

query.awaitTermination()
