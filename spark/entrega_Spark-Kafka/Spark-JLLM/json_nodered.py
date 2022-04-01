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
        StructField('tind', StringType(), True),
        StructField('pre', FloatType(), True),
        StructField('cate', StringType(), True),
        StructField('pro', StringType(), True)
    ]
)

# Creo un novo dataframe no que extraio de value todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

df2.createOrReplaceTempView('vendas')
consulta = spark.sql('SELECT tind, SUM(pre) as beneficio, COUNT(*) as cantidad_registros FROM vendas GROUP BY tind')
# crear unha vista 
# facer consultas SQL 
# consultas que implican agregación -> .outputMode("complete"/"append"/...)

query = consulta \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='59 seconds') \
    .start()

query.awaitTermination()
