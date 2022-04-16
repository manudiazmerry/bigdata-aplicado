from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

appName = "Visor de productos mais vendidos"
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


df = df.selectExpr("CAST(value AS STRING)")

# Defino o schema dos JSON codificado no campo value
schema = StructType(
    [
        StructField('tenda', StringType(), True),
        StructField('categoria', StringType(), True),
        StructField('producto', StringType(), True),
        StructField('cantidade', FloatType(), True)
    ]
)

# Creo un novo dataframe no que extraio de value todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

#Facer consultas SQL
df2.createOrReplaceTempView("pedidos")
df_sql = spark.sql("SELECT * FROM pedidos")

# Stream de escritura
query = df_sql \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", "/home/hduser/Escritorio/Exercicio_Kafka/Outputs/checkpoint/") \
    .option("path", "/home/hduser/Escritorio/Exercicio_Kafka/Outputs/") \
    .start()

query.awaitTermination()

