from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

appName = "Script Visor de productos"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
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

#procesado

df = df.selectExpr("CAST(value AS STRING)")

schema = StructType(
    [
        StructField('tipo', StringType(), True),
	StructField('producto', StringType(), True),
	StructField('prezo', StringType(), True),
	StructField('tenda', StringType(), True),
        StructField('categoria', StringType(), True)
    ]
)

df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

#Na vista temporal de vendas, mostramos os datos que queremos
# do streaming cada 5s con outputMode Complete
df2.createOrReplaceTempView('vendas')
df_sql = spark.sql('SELECT producto, count(producto), sum(prezo) FROM vendas GROUP BY producto ORDER by count(producto) DESC')

# Stream de escritura
# opción truncate = false para poder ver as datas e horas
query = df_sql \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode('complete') \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()
