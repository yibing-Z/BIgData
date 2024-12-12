from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, to_date
from pyspark.sql.window import Window

# 初始化 SparkSession
spark = SparkSession.builder.appName("CityTopUsersByTraffic").getOrCreate()

# 读取用户余额表和用户信息表
df_balance = spark.read.option("header", "true").csv("file:///home/user/spark_code/user_balance_table.csv")
df_profile = spark.read.option("header", "true").csv("file:///home/user/spark_code/user_profile_table.csv")

df_balance = df_balance.withColumn("date", to_date(col("report_date"), "yyyyMMdd"))
df_balance_august = df_balance.filter(col("date").between("2014-08-01", "2014-08-31"))

df_balance_august = df_balance_august.withColumn(
    "total_traffic", col("total_purchase_amt") + col("total_redeem_amt")
)
df_user_city = df_balance_august.join(
    df_profile,
    df_balance_august.user_id == df_profile.user_id,
    "inner"
).select(
    df_profile.city,
    df_balance_august.user_id,
    df_balance_august.total_traffic
)

city_user_traffic = df_user_city.groupBy("city", "user_id").agg(sum("total_traffic").alias("total_traffic"))

window_spec = Window.partitionBy("city").orderBy(desc("total_traffic"))

city_user_ranked = city_user_traffic.withColumn("rank", row_number().over(window_spec))


top_users_per_city = city_user_ranked.filter(col("rank") <= 3)

output_path = "file:///home/user/spark_code/output2_2"
top_users_per_city.select("city", "user_id", "total_traffic") \
    .rdd.map(lambda row: f"{row['city']} {row['user_id']} {row['total_traffic']}") \
    .coalesce(1).saveAsTextFile(output_path)

spark.stop()
