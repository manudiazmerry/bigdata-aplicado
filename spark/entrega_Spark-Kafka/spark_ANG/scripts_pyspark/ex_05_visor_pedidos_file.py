from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

appName = "Script Visor de productos"
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
    .read \
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
	StructField('cantidade', StringType(), True),
	StructField('tenda', StringType(), True),
        StructField('categoria', StringType(), True)
    ]
)

df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

#Creamos a vista temporal de pedidos e mostramos no archivo os 
#  datos que había no topic nese momento, en formato JSON.
#  en vez de utilizar "readStream" e "writeStream" utilizamos read e write
df2.createOrReplaceTempView('pedidos')
df_sql = spark.sql('SELECT * FROM pedidos')

# Stream de escritura
# opción truncate = false para poder ver as datas e horas
query = df_sql \
    .write \
    .json("/home/hduser/pedidos.json") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()

