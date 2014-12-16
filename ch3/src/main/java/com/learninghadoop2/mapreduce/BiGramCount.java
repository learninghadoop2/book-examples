package com.learninghadoop2.mapreduce;
import java.io.* ;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BiGramCount extends Configured implements Tool
{
   public static class BiGramMapper
           extends Mapper<Object, Text, Text, IntWritable> {
       private final static IntWritable one = new IntWritable(1);
       private Text word = new Text();

       @Override
       protected void setup(Context context) throws IOException, InterruptedException {

       }

       public void map(Object key, Text value, Context context
       ) throws IOException, InterruptedException {
           String[] words = value.toString().split(" ");

           Text bigram = new Text();
           String prev = null;

           for (String s : words) {
               if (prev != null) {
                   bigram.set(prev + "\t+\t" + s);
                   context.write(bigram, one);
               }

               prev = s;
           }
       }
   }

    @Override
    public int run(String[] args) throws Exception {
         Configuration conf = getConf();

         args = new GenericOptionsParser(conf, args).getRemainingArgs();

         Job job = Job.getInstance(conf);
         job.setJarByClass(BiGramCount.class);
         job.setMapperClass(BiGramMapper.class);
         job.setReducerClass(IntSumReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BiGramCount(), args);
        System.exit(exitCode);
    }

}

