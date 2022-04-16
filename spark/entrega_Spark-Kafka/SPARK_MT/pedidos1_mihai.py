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

# Stream de lectura
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .load()

# casteo a lectura a STRING (a info en kafka circula en binario)
df = df.selectExpr("CAST(value AS STRING)")

# Defino o schema dos JSON codificado no campo value
schema = StructType(
    [
        StructField('Tienda', StringType(), True),
        StructField('categoria', StringType(), True),
        StructField('producto', StringType(), True),
        StructField('cantidade', FloatType(), True)
    ]
)

# Creo un novo dataframe no que extraio de value todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

#Creo la tabla ara hacer la consulta SQL
df2.createOrReplaceTempView('pedidos')
#Hago la consulta
consulta=spark.sql("SELECT * FROM pedidos")

# Stream de escritura
#Hago que añada las nuevas lineas y muestre el resultado por pantalla cada 10 segundos
query = consulta \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .format("console") \
    .option("truncate", False) \
    .start()


query.awaitTermination()
