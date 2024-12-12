from pyspark import SparkContext, SparkConf
from datetime import datetime

# 初始化 SparkContext
conf = SparkConf().setAppName("Active Users in August 2014").setMaster("local[4]")
sc = SparkContext(conf=conf)

# 数据路径
data_path = "file:///home/user/spark_code/user_balance_table.csv"
output_path = "file:///home/user/spark_code/output1_2"

raw_data = sc.textFile(data_path)

def parse_and_clean(line):
    if line.startswith("user_id"):  # 跳过标题行
        return None
    parts = line.split(",")
    user_id = parts[0]
    date_str = parts[1]
    try:
        date = datetime.strptime(date_str, '%Y%m%d').date()
        return (user_id, date)
    except ValueError:
        return None  

cleaned_data = raw_data.map(parse_and_clean).filter(lambda x: x is not None)

august_data = cleaned_data.filter(lambda x: x[1].year == 2014 and x[1].month == 8)

user_activity = august_data.map(lambda x: (x[0], x[1])).groupByKey().mapValues(lambda dates: len(set(dates)))
active_users = user_activity.filter(lambda x: x[1] >= 5)
active_user_count = active_users.count()
result = sc.parallelize([f"活跃用户总数: {active_user_count}"])
result.repartition(1).saveAsTextFile(output_path)

sc.stop()
