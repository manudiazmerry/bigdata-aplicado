from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

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
        StructField('tenda', StringType(), True),
        StructField('categoria', StringType(), True),
	StructField('productos', StringType(), True),
        StructField('cantidades', StringType(), True)
    ]
)

# Creo un novo dataframe no que extraio de value todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

# crear unha vista 
# facer consultas SQL 
# consultas que implican agregación -> .outputMode("complete"/"append"/...)
df2.createOrReplaceTempView("pedidos")
df_sql = spark.sql("SELECT tenda, COUNT(productos) as total_pedidos, SUM(cantidades) as cantidade FROM pedidos GROUP BY tenda")

# Stream de escritura
# opción truncate = false para poder ver as datas e horas
query = df_sql \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()
