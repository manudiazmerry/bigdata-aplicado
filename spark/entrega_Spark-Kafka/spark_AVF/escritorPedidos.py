from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

appName = "Escritor de pedidos"
master = "local"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

kafka_servers = "kafkaserver:9092"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "pedidos") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

# Defino o schema dos JSON codificado no campo value
schema = StructType(
    [
        StructField('tienda', StringType(), True),
        StructField('fecha', StringType(), True),
        StructField('categoria', StringType(), True),
        StructField('producto', StringType(), True),
        StructField('cantidad', StringType(), True)
    ]
)

# Creo un novo dataframe no que extraio de value todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))


df2.createOrReplaceTempView('ventas')

consulta = spark.sql("select * from ventas")

query = consulta\
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option('header',True)\
    .option('path',"file:///home/hduser/Documentos/entrega/output/pedidos.csv") \
    .option("checkpointLocation", "file:///home/hduser/Documentos/entrega/output/.checkpoint/filesink_checkpoint") \
    .start()

query.awaitTermination()

