from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas

spark = SparkSession.builder.getOrCreate()

df_general = None
df_stationlog = None

def process_files(log_file):
    # global df_general
    # global df_stationlog
    if 'stationlog' in log_file:
        file_name_read = '/stationlog/' + log_file
        file_name_write = '/new_stationlog_spark/' + log_file
    else:
        file_name_read = '/list_h_3/' + log_file
        file_name_write = '/new_files_3_spark/' + log_file
    df = spark.read.option("header", True).csv(file_name_read)
    if 'list_h_3' in file_name_read:
        df = df.where(df.Day == 7)
        df = df.withColumnRenamed('\tSpeed', 'Speed')
    else:
        df = df.withColumnRenamed('\tError', 'Error')
    df = df.withColumnRenamed('\tTickTime', 'TickTime')
    if 'stationlog' in log_file:
        df = df.where(df.TickTime <= 86400)
    df = df.withColumn("TickTime", df.TickTime / 300)
    df = df.withColumn("TickTime", df.TickTime.cast('Integer'))
    if 'list_h_3' in file_name_read:
        df = df.withColumn("Speed", df.Speed.cast('Double'))
        df = df.groupby(['Day', 'TickTime']).sum("Speed")
        df = df.withColumnRenamed('sum(Speed)', 'Speed')
    else:
        df = df.withColumn('Error', df.Error.cast('Integer'))
        df = df.groupby(['Day', 'TickTime']).sum('Error')
    return df


name_of_h = 'h96'

df = spark.read.text(f'/name_of_files_3/{name_of_h}.txt')

for i in range(21):
    file = df.collect()[i][0]
    print(file)
    if df_general is None and 'userlog' in file:
        df_general = process_files(file)
    elif 'userlog' in file and df_general is not None:
        df_general = df_general.union(process_files(file))
    elif 'stationlog' in file:
        df_stationlog = process_files(file)

# df_general = df_general.join(df_stationlog, df_general.TickTime == df_stationlog.TickTime, "inner")
df_general = df_general.groupby(['Day', 'TickTime']).sum('Speed')
print(df_general.show())
df_general = df_general.join(df_stationlog, ["TickTime", "Day"], "inner")
print(df_general.show())
df_general.coalesce(1).write.format('csv').save(f"/new_files_3_spark/{name_of_h}", header = 'true')


