# Visor de produtos que máis se venden e "beneficios".
# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Asignamos o nome do servidor, o tópico
appName = "Exercicio 3"
master = "local"
KAFKA_SERVERS = "kafkaserver:9092"
TOPIC = "vendas"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# Pasamos o server, topic no df
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .load()
    
df = df.selectExpr("CAST(value AS STRING)")
# Creamos a estrutura do df
schema = StructType(
    [
       StructField('tenda', StringType(), True),
       StructField('categoria', StringType(), True),
       StructField('produto', StringType(), True),
       StructField('prezo', FloatType(),True)
    ])

df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

df2.createOrReplaceTempView('vendas')
# Mostramos o produto, contamos o número total de produtos e imos sumando os prezos de todos os produtos
# Despois ordenamos os produtos polo número total de produtos para que os ordene de mais a menos
consulta = spark.sql('SELECT produto, COUNT (*) AS numero, ROUND(SUM(prezo), 2) AS beneficio FROM vendas GROUP BY produto ORDER BY numero DESC')

query = consulta \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
