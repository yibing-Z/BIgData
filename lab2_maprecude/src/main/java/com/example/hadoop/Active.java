package main.java.com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.example.hadoop.Fund;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import java.io.IOException;

public class Active{

    public static class ActiveUserMapper extends Mapper<Object, Text, Text, Text> {
        private Text userKey = new Text();  // 用户 ID
    
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
    
            // 检查是否是标题行
            if (line.startsWith("user_id")) {  // 假设标题行以 "user_id" 开头
                return;  // 跳过标题行
            }
    
            String[] fields = line.split(",");  // 假设字段是用逗号分隔的
    
            if (fields.length < 9) { // 确保有足够的字段
                return; // 忽略无效行
            }
    
            String user = fields[0].trim();  // 用户 ID
            String direct_purchase_amt = fields[8].trim();  // 直接购买金额
            String total_redeem_amt = fields[5].trim();  // 赎回金额
            // 如果字段缺失，视为零
            if (direct_purchase_amt.isEmpty()) {
                direct_purchase_amt = "0";
            }
            if (total_redeem_amt.isEmpty()) {
                total_redeem_amt = "0";
            }
            double directPurchaseAmt = 0;
            double totalRedeemAmt = 0;
    
            // 使用 try-catch 块捕获解析异常
            try {
                directPurchaseAmt = Double.parseDouble(direct_purchase_amt);
            } catch (NumberFormatException e) {
                System.err.println("Unable to parse direct_purchase_amt: " + direct_purchase_amt);
                return;  // 跳过该行
            }
    
            try {
                totalRedeemAmt = Double.parseDouble(total_redeem_amt);
            } catch (NumberFormatException e) {
                System.err.println("Unable to parse total_redeem_amt: " + total_redeem_amt);
                return;  // 跳过该行
            }
    
            userKey.set(user);
            if (directPurchaseAmt > 0 || totalRedeemAmt > 0) {
                context.write(userKey, new Text("1"));  // 活跃用户输出 "1"
            }
        }
    }
    public static class ActiveUserReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private List<UserActivity> userActivityList = new ArrayList<>();

    // 自定义类用于存储用户ID和活跃天数
        class UserActivity {
            String userId;
            int activeDays;

            UserActivity(String userId, int activeDays) {
                this.userId = userId;
                this.activeDays = activeDays;
            }
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum += Integer.parseInt(val.toString()); // 将 Text 转换为 String，再解析为 int
            }
            userActivityList.add(new UserActivity(key.toString(), sum));
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 对用户按活跃天数进行降序排序
            Collections.sort(userActivityList, new Comparator<UserActivity>() {
                @Override
                public int compare(UserActivity u1, UserActivity u2) {
                    return Integer.compare(u2.activeDays, u1.activeDays); // 降序
                }
            });
             // 输出排序后的结果
            for (UserActivity userActivity : userActivityList) {
                context.write(new Text(userActivity.userId), new Text(String.valueOf(userActivity.activeDays)));
            }
        }
    } 

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: Fund <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf, "Active user count");
        job.setJarByClass(Active.class);
        job.setMapperClass(ActiveUserMapper.class); // 指定正确的 Mapper 类
        job.setReducerClass(ActiveUserReducer.class); // 指定正确的 Reducer 类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

