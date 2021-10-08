from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas

spark = SparkSession.builder.getOrCreate()

name_of_files = ['h7', 'h15', 'h85', 'h96']
file_name_read = '/new_files_3_spark/'

first_h = name_of_files.pop()
df = spark.read.option("header", True).csv(file_name_read + first_h)
df = df.withColumnRenamed('sum(Speed)', 'Speed')
df = df.withColumnRenamed('sum(Error)', 'Error')
df = df.withColumn("Speed", df.Speed.cast('Double'))
df = df.withColumn("Error", df.Error.cast('Integer'))
df = df.toPandas()
ax = df.plot(x='Error', y='Speed', marker='o', label = first_h)

for i in name_of_files:
    df = spark.read.option("header", True).csv(file_name_read + i)
    df = df.withColumnRenamed('sum(Speed)', 'Speed')
    df = df.withColumnRenamed('sum(Error)', 'Error')
    df = df.withColumn("Speed", df.Speed.cast('Double'))
    df = df.withColumn("Error", df.Error.cast('Integer'))
    df = df.toPandas()
    print(df)
    df.plot(x='Error', y='Speed', ax=ax, marker='o', label = i)

# plt.show()
plt.title('Скорости и ошибки в одни и те же моменты времени')
plt.xlabel('Скорости')
plt.ylabel('Ошибки')
plt.legend()
plt.savefig('result.png')
