package com.learninghadoop2.mapreduce;

import java.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;

public class TopTenHashTag extends Configured implements Tool
{
    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text>
    {
        private TreeMap<Integer, Text> topN = new TreeMap<Integer, Text>();

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // (hashtag, count) tuple
            String[] words = value.toString().split("\t") ;
                if (words.length < 2) {
                    return;
            }

            topN.put(Integer.parseInt(words[1]), new Text(value));

            if (topN.size() > 10) {
                    topN.remove(topN.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (Text t : topN.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }
    public static class TopTenReducer extends
            Reducer<NullWritable, Text, NullWritable, Text> {

        private TreeMap<Integer, Text> topN = new TreeMap<Integer, Text>();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] words = value.toString().split("\t") ;

                topN.put(Integer.parseInt(words[1]),
                    new Text(value));

                if (topN.size() > 10) {
                    topN.remove(topN.firstKey());
                }
            }

            for (Text word : topN.descendingMap().values()) {
                context.write(NullWritable.get(), word);
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf);

        job.setJarByClass(HashTagCount.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.SetNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TopTenHashTag(), args);
        System.exit(exitCode);
    }
}
