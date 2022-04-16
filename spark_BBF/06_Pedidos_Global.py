from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

appName = "Pedidos"
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
        StructField('producto', StringType(), True),
        StructField('precio', FloatType(), True),
        StructField('categoria', StringType(), True),
        StructField('cantidad', FloatType(), True),
        StructField('tienda', StringType(), True)
    ]
)

# Creo un novo dataframe no que extraio de value todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

#Facer consultas SQL
df2.createOrReplaceTempView("Ped")
#df_sql = spark.sql("SELECT tienda, COUNT(producto) as NumVentas, SUM(precio) as Beneficios FROM ventar GROUP BY tienda")
df_sql = spark.sql("SELECT * FROM Ped")


# Stream de escritura
query = df_sql \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()
