package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jline.utils.InputStreamReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Comparator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;
import java.util.List;
import java.util.ArrayList;

// Main class to encapsulate the Mapper, Reducer, and Driver logic
public class WordCountJob {

    // Mapper class
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Set<String> stopWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load stop-word list from command-line argument
        String stopWordsFilePath = context.getConfiguration().get("stopwords.file");
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream inputStream = null;
        BufferedReader br = null;

        try {
            inputStream = fs.open(new Path(stopWordsFilePath));
            br = new BufferedReader(new InputStreamReader(inputStream));  // 使用 InputStreamReader
            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line.trim().toLowerCase());
            }
        } finally {
            IOUtils.closeStream(br);
            IOUtils.closeStream(inputStream);
        }
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split CSV file
        String[] fields = value.toString().split(",");
    
        // Check if there are enough fields
        if (fields.length > 2) {  // 至少需要有 3 列
            // 拼接剩余列内容，去掉第一列和最后一列
            StringBuilder headlineBuilder = new StringBuilder();
            for (int i = 1; i < fields.length - 2; i++) {
                if (i > 1) {
                    headlineBuilder.append(",");  // 在字段之间添加逗号
                }
                headlineBuilder.append(fields[i].trim());
            }
            
            // 转换为小写并移除标点符号
            String headline = headlineBuilder.toString().toLowerCase().replaceAll("[^a-zA-Z\\s]", "");
    
            // 使用 StringTokenizer 进行分词
            StringTokenizer tokenizer = new StringTokenizer(headline);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (!stopWords.contains(token)) {
                    word.set(token);
                    context.write(word, one); // Emit <word, 1>
                }
            }
        }
    }    
}

    // Reducer class
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> wordCountMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            wordCountMap.put(key.toString(), sum);
        }

        @Override
protected void cleanup(Context context) throws IOException, InterruptedException {
    // Priority queue to maintain top 100 frequent words
    PriorityQueue<Map.Entry<String, Integer>> topWords = new PriorityQueue<>(
        new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());  // Ascending order by count
            }
        });

    // Add to priority queue and maintain size <= 100
    for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
        topWords.offer(entry);
        if (topWords.size() > 100) {
            topWords.poll();  // Remove smallest entry if size exceeds 100
        }
    }

    // Collect top words to a list to sort them later
    List<Map.Entry<String, Integer>> sortedTopWords = new ArrayList<>(topWords);
    // Sort in descending order by frequency
    sortedTopWords.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

    // Output the top 100 words in descending order of frequency
    for (Map.Entry<String, Integer> entry : sortedTopWords) {
        context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
    }
}
    }

    // Optional Combiner class to reduce network traffic
    public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));  // Emit <word, sum>
        }
    }

    // Main driver class
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: WordCountJob <input path> <output path> <stop-words file>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("stopwords.file", args[2]);  // Set the stop-words file path in the configuration
        Job job = Job.getInstance(conf, "Word Count with Top 100 Frequent Words");

        job.setJarByClass(WordCountJob.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountCombiner.class);  // Set combiner class
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
