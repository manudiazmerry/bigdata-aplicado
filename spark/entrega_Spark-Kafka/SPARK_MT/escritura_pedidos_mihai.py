from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

#Hago que me guarde cada pedido a un .csv

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

#Creo la tabla para hacerle las consultas SQL
df2.createOrReplaceTempView('pedidos')
#Hago la consulta
consulta=spark.sql("SELECT * FROM pedidos")

# Stream de escritura
# En este código guardo los resultados obtenidos a un .csv
query = consulta \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option('header',True) \
    .option('path',"file:///home/hduser/Evaluacion/resultado.csv") \
    .option("checkpointLocation", "file:///home/hduser/Evaluacion/filesink_checkpoint") \
    .start()

query.awaitTermination()
