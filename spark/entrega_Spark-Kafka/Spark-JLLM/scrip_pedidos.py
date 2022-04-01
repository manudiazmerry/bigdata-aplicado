from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

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
        StructField('bar', StringType(), True),
        StructField('precio', FloatType(), True),
        StructField('producto', StringType(), True),
        StructField('cantidad', StringType(), True),
        StructField('categoria', StringType(), True)
    ]
)

# Creo un novo dataframe no que extraio de value todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

df2.createOrReplaceTempView('pedidos')
consulta = spark.sql('SELECT * FROM pedidos')



query = consulta \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='59 seconds') \
    .start()

query.awaitTermination()
