package main.java.com.example.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat; // 使用 KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Constellar {

    public static class ActiveDaysMapper extends Mapper<Text, Text, Text, LongWritable> { // 修改 LongWritable 为 Text
        private Map<String, String> zodiacMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path path = new Path(conf.get("zodiac.file.path"));
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields.length >= 4) { // 确保有足够的字段
                    String userId = fields[0];
                    String zodiacSign = fields[3]; // 假设星座在第四列
                    zodiacMap.put(userId, zodiacSign);
                } else {
                    System.err.println("Invalid line format: " + line);
                }
            }

            reader.close();
            inputStream.close();
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException { // 修改 LongWritable 为 Text
            String userId = key.toString();
            long activeDays = Long.parseLong(value.toString()); // 将 Text 转换为 long

            String zodiacSign = zodiacMap.get(userId);

            if (zodiacSign != null) {
                context.write(new Text(zodiacSign), new LongWritable(activeDays));
            }
        }
    }

    public static class AvgActiveDaysReducer extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long totalActiveDays = 0;
            int count = 0;

            for (LongWritable value : values) {
                totalActiveDays += value.get();
                count++;
            }

            double averageActiveDays = count > 0 ? (double) totalActiveDays / count : 0;
            context.write(key, new Text(String.format("%.2f", averageActiveDays)));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Constellar <input path> <output path> <zodiac file path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("zodiac.file.path", args[2]); // 从命令行获取星座文件路径

        Job job = Job.getInstance(conf, "Zodiac Average Active Days");
        job.setJarByClass(Constellar.class);

        job.setMapperClass(ActiveDaysMapper.class);
        job.setReducerClass(AvgActiveDaysReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class); // 设置为 KeyValueTextInputFormat
        job.setOutputFormatClass(TextOutputFormat.class); // 设置输出格式

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 使用 FileInputFormat 设置输入路径
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1])); // 设置输出路径

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
