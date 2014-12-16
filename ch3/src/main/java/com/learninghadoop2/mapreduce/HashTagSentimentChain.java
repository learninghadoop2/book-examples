package com.learninghadoop2.mapreduce;
import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class HashTagSentimentChain extends Configured implements Tool
{

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        // location (on hdfs) of the positive words list
        conf.set("job.positivewords.path", args[2]);
        conf.set("job.negativewords.path", args[3]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(HashTagSentimentChain.class);

        Configuration loweCaseMapperConf = new Configuration(false);
        ChainMapper.addMapper(job,
                LowerCaseMapper.class,
                LongWritable.class, Text.class,
                IntWritable.class, Text.class,
                loweCaseMapperConf);

        Configuration hashTatSentimentConf = new Configuration(false);
        ChainMapper.addMapper(job,
                HashTagSentiment.HashTagSentimentMapper.class,
                IntWritable.class,
                Text.class, Text.class,
                Text.class,
                hashTatSentimentConf);


        job.setReducerClass(HashTagSentiment.HashTagSentimentReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main (String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HashTagSentimentChain(), args);
        System.exit(exitCode);
    }
}



