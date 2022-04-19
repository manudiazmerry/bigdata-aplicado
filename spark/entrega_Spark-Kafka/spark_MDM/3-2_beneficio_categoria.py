from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

appName = "lectura en kafka de stream tipo json"
master = "local"
KAFKA_SERVERS = "kafka:9092"
TOPIC = "vendas"

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

# pasamos de binario a string (los flujos en kafka circulan en binario)
df = df.selectExpr("CAST(value AS STRING)")

# Defino o schema dos JSON codificado no campo value
schema = StructType(
    [
        StructField('tienda', StringType(), True),
        StructField('marca', StringType(), True),
        StructField('modelo', StringType(), True),
        StructField('color', StringType(), True),
        StructField('energia', StringType(), True),
        StructField('precio', FloatType(), True)
    ]
)

# Creo un novo dataframe no que extraio de value todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

# Creo la tabla sobre el dataframe para poder hacerle consultas SQL
df2.createOrReplaceTempView('vendas')

# Hacemos la consulta
consulta=spark.sql("SELECT marca, sum(precio) AS beneficio FROM vendas GROUP BY marca")


# Stream de escritura

query = consulta \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
