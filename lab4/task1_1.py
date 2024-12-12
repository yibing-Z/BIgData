from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Daily Capital Flow").setMaster("local[4]")
sc = SparkContext(conf=conf)

data_path = "file:///home/user/spark_code/user_balance_table.csv"  
output_path = "file:///home/user/spark_code/output1"  

raw_data = sc.textFile(data_path)

def parse_and_clean(line):
    if line.startswith("user_id"):
        return None

    parts = line.split(",")
    
    if len(parts) < 9:
        return None 
    
    date = parts[1]

    try:
        total_purchase_amt = float(parts[4]) if parts[4] else 0.0 
        total_redeem_amt = float(parts[8]) if parts[8] else 0.0 
    except ValueError:
        return None  
    
    return (date, (total_purchase_amt, total_redeem_amt))

cleaned_data = raw_data.map(parse_and_clean).filter(lambda x: x is not None)
daily_flow = cleaned_data.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
result = daily_flow.map(lambda x: f"{x[0]},{x[1][0]},{x[1][1]}")
result.saveAsTextFile(output_path)
sc.stop()