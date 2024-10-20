package main.java.com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat; // 导入 KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import org.apache.hadoop.io.DoubleWritable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class WeeklyFundFlow {

    // Mapper 类
    public static class FundFlowMapper extends Mapper<Text, Text, Text, Text> {
        private Text weekdayKey = new Text(); // 星期几
        private Text flowValues = new Text();  // 资金流入量和流出量

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String dateStr = key.toString().trim(); // 日期 (key)
            String[] flowAmounts = value.toString().split(","); // 输入格式 v,v

            if (flowAmounts.length < 2) {
                return; // 忽略无效行
            }

            String total_purchase_amt = flowAmounts[0].trim(); // 资金流入
            String total_redeem_amt = flowAmounts[1].trim(); // 资金流出
            try {
                String weekday = getWeekday(dateStr);
                weekdayKey.set(weekday);
                flowValues.set(total_purchase_amt + "," + total_redeem_amt);
                context.write(weekdayKey, flowValues);
            } catch (DateTimeParseException e) {
                // 记录异常，但不使任务失败
                System.err.println("Invalid date format for input: " + dateStr);
            }       
        }

        // 辅助方法：将字符串日期转换为星期几
        private String getWeekday(String dateStr) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            LocalDate date = LocalDate.parse(dateStr, formatter);
            return date.getDayOfWeek().name(); // 获取星期几的名称
        }
    }

    // Reducer 类
    public static class FundFlowReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private List<WeekData> weekDataList = new ArrayList<>();
    
        // 将 WeekData 类声明为 static
        public static class WeekData {
            String week;
            double avgInflow;
            double avgOutflow;
    
            WeekData(String week, double avgInflow, double avgOutflow) {
                this.week = week;
                this.avgInflow = avgInflow;
                this.avgOutflow = avgOutflow;
            }
        }
    
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalInflow = 0.0;
            int count = 0;
            double totalOut = 0.0;
            for (Text val : values) {
                String[] flowAmounts = val.toString().split(",");
                double inflow = Double.parseDouble(flowAmounts[0]); // 获取资金流入量
                double outflow = Double.parseDouble(flowAmounts[1]);
                totalInflow += inflow;
                totalOut += outflow;
                count++;
            }
    
            // 计算平均值
            double avgInflow = totalInflow / count;
            double avgOut = totalOut / count;
    
            // 将平均值和星期添加到列表中
            weekDataList.add(new WeekData(key.toString(), avgInflow, avgOut));
        }
    
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 对列表进行排序，按 avgInflow 从大到小排序
            Collections.sort(weekDataList, new Comparator<WeekData>() {
                public int compare(WeekData w1, WeekData w2) {
                    return Double.compare(w2.avgInflow, w1.avgInflow); // 降序排序
                }
            });
    
            // 输出排序后的结果
            for (WeekData weekData : weekDataList) {
                result.set(weekData.avgInflow + "," + weekData.avgOutflow);
                context.write(new Text(weekData.week), result);
            }
        }
    }
    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weekly fund flow");
        job.setJarByClass(WeeklyFundFlow.class);

        // 使用 KeyValueTextInputFormat 作为输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(FundFlowMapper.class);
        job.setReducerClass(FundFlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
