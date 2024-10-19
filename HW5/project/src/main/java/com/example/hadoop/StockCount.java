package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Comparator;

public class StockCount {

    public static class StockMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text stockCode = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // 使用逗号分隔行，但股票代码是最后一列，前面的字段可能包含逗号
            String[] fields = line.split(",", -1);
            
            if (fields.length > 0) {
                String stockCodeValue = fields[fields.length - 1].trim(); // 获取最后一列作为股票代码
                if (!stockCodeValue.isEmpty()) {
                    stockCode.set(stockCodeValue);
                    context.write(stockCode, one); // 输出<股票代码，1>
                }
            }
        }
    }

    public static class StockReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // 用于保存股票代码和对应计数的Map
        private Map<String, Integer> stockCountMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            stockCountMap.put(key.toString(), sum);  // 保存每个股票代码和对应的计数
        }

        // 在Reducer完成所有reduce任务后执行
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 使用TreeMap根据值的大小进行降序排序
            TreeMap<String, Integer> sortedMap = new TreeMap<>(new Comparator<String>() {
                public int compare(String key1, String key2) {
                    int compare = stockCountMap.get(key2).compareTo(stockCountMap.get(key1)); // 按值降序
                    if (compare == 0) {
                        return key1.compareTo(key2); // 如果值相同，按股票代码字母顺序升序
                    }
                    return compare;
                }
            });
            sortedMap.putAll(stockCountMap);  // 将所有元素放入排序的TreeMap

            // 输出排序后的结果
            for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: StockCount <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf, "stock count");
        job.setJarByClass(StockCount.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
