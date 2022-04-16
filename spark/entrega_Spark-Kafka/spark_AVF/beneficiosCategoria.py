from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

appName = "Beneficios por Categoria"
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
    .option("subscribe", "ventas") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

# Defino o schema dos JSON codificado no campo value
schema = StructType(
    [
        StructField('tienda', StringType(), True),
        StructField('fecha', StringType(), True),
        StructField('categoria', StringType(), True),
        StructField('producto', StringType(), True),
        StructField('precio', StringType(), True)
    ]
)

# Creo un novo dataframe no que extraio de value todos os campos en formato JSON
df2 = df.withColumn("value", from_json("value", schema)).select(col('value.*'))

df2.createOrReplaceTempView('ventas')
#Muestro el dinero ingresado por cada categoria
consulta = spark.sql("select categoria, cast(sum(precio) as decimal(30,2)) as beneficio from ventas group by categoria order by beneficio DESC")

query = consulta \
    .writeStream \
    .outputMode("complete")\
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()

