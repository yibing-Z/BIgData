from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Average Balance ") \
    .master("local[4]") \
    .getOrCreate()

balance_data_path = "file:///home/user/spark_code/user_balance_table.csv"
profile_data_path = "file:///home/user/spark_code/user_profile_table.csv"

df_balance = spark.read.option("header", "true").csv(balance_data_path)
df_profile = spark.read.option("header", "true").csv(profile_data_path)
df_balance = df_balance.withColumn("tBalance", col("tBalance").cast("float"))
df_balance = df_balance.withColumn("date", to_date(col("report_date"), "yyyyMMdd"))
march_1_2014_data = df_balance.filter((col("date") == "2014-03-01"))
df_joined = march_1_2014_data.join(df_profile, on="user_id", how="inner")
city_avg_balance = df_joined.groupBy("city").agg({"tBalance": "avg"})
sorted_avg_balance = city_avg_balance.orderBy(col("avg(tBalance)").desc())
sorted_avg_balance.select("city", "avg(tBalance)").show()
output_path = "file:///home/user/spark_code/output2_1"
sorted_avg_balance.select("city", "avg(tBalance)") \
    .rdd.map(lambda row: f"{row['city']} {row['avg(tBalance)']}") \
    .coalesce(1).saveAsTextFile(output_path)

spark.stop()
