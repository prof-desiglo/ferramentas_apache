#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Criar SparkSession conectando ao master
spark = SparkSession.builder \
    .appName("DockerSparkTest") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

print("Spark conectado ao cluster!")

# Teste básico
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])

print("DataFrame criado:")
df.show()

# Operação de agregação
result = df.groupBy().agg(
    avg("Age").alias("average_age"),
    count("*").alias("total_people")
)

print("Resultado da agregação:")
result.show()

spark.stop()
print("Teste concluído com sucesso!")