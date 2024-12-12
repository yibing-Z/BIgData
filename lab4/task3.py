from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr, lit
from pyspark.sql.types import IntegerType, DateType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
spark = SparkSession.builder.appName("Date_Regression").getOrCreate()

user_balance = spark.read.csv("file:///home/user/spark_code/user_balance_table.csv", header=True, inferSchema=True)
user_balance = user_balance.withColumn("report_date", to_date(col("report_date"), "yyyyMMdd"))

daily_balance = user_balance.groupBy("report_date") \
                            .agg({"total_purchase_amt": "sum", "total_redeem_amt": "sum"}) \
                            .withColumnRenamed("sum(total_purchase_amt)", "total_purchase") \
                            .withColumnRenamed("sum(total_redeem_amt)", "total_redeem")
daily_balance = daily_balance.withColumn(
    "day_index", 
    expr("datediff(report_date, '2014-01-01')")
)

feature_cols = ["total_purchase", "total_redeem"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
daily_balance = assembler.transform(daily_balance)

train_data = daily_balance.filter(col("report_date") < "2014-09-01")

lr = LinearRegression(featuresCol="features", labelCol="day_index")
lr_model = lr.fit(train_data)

future_dates = spark.createDataFrame(
    [(i,) for i in range(243, 243 + 30)],  # 243 = day_index for 2014-09-01
    schema=["day_index"]
)

avg_values = daily_balance.select(
    expr("mean(total_purchase)").alias("total_purchase"),
    expr("mean(total_redeem)").alias("total_redeem")
).first()

future_dates = future_dates.withColumn("total_purchase", lit(avg_values["total_purchase"])) \
                           .withColumn("total_redeem", lit(avg_values["total_redeem"]))

future_dates = assembler.transform(future_dates)

predictions = lr_model.transform(future_dates).select("day_index", "prediction")
predictions.write.csv("tc_comp_predict_table.csv", header=True)
spark.stop()
