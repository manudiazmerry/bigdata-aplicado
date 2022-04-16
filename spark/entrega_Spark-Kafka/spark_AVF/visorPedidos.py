from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

appName = "Visor de pedidos"
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
#Hago la consulta para mostrar los pedidos agrupados por tienda y producto
consulta = spark.sql("select tienda, producto, cast(sum(cantidad) as Integer) as cantidad, cast(fecha as timestamp) from ventas group by tienda, producto, fecha order by tienda, cantidad DESC")

query = consulta \
    .writeStream \
    .outputMode("complete")\
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()

