package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Fund {

    public static class FundFlowMapper extends Mapper<Object, Text, Text, Text> {
        private Text dateKey = new Text();  // 日期
        private Text flowValues = new Text();  // 资金流入量和流出量

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");  // 假设字段是用逗号分隔的

            if (fields.length < 6) { // 确保有足够的字段
                return; // 忽略无效行
            }

            String mfd_date = fields[1].trim();  // 日期
            String total_purchase_amt = fields[4].trim();  // 资金流入
            String total_redeem_amt = fields[5].trim();  // 资金流出

            // 如果字段缺失，视为零
            if (total_purchase_amt.isEmpty()) {
                total_purchase_amt = "0";
            }
            if (total_redeem_amt.isEmpty()) {
                total_redeem_amt = "0";
            }

            // 输出格式：<日期>    <资金流入量,资金流出量>
            dateKey.set(mfd_date);
            flowValues.set(total_purchase_amt + "," + total_redeem_amt);
            context.write(dateKey, flowValues);
        }
    }

    public static class FundFlowReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalInflow = 0.0;
            double totalOutflow = 0.0;
        
            for (Text val : values) {
                String[] flowAmounts = val.toString().split(",");
                
                // 确保分割后数组长度为2
                if (flowAmounts.length != 2) {
                    System.err.println("Invalid input for reduce: " + val.toString());
                    continue; // 如果格式不正确，跳过此条记录
                }
        
                try {
                    // 尝试解析资金流入和流出的值
                    double inflow = Double.parseDouble(flowAmounts[0]);
                    double outflow = Double.parseDouble(flowAmounts[1]);
        
                    // 仅在解析成功后累加
                    totalInflow += inflow;
                    totalOutflow += outflow;
                } catch (NumberFormatException e) {
                    System.err.println("Skipping invalid numbers: " + val.toString());
                    continue; // 跳过无法解析的值
                }
            }
        
            // 输出最终的资金流入和流出总额
            result.set(totalInflow + "," + totalOutflow);
            context.write(key, result);
        }        
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: Fund <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf, "fund flow count");
        job.setJarByClass(Fund.class);
        job.setMapperClass(FundFlowMapper.class); // 指定正确的 Mapper 类
        job.setReducerClass(FundFlowReducer.class); // 指定正确的 Reducer 类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
