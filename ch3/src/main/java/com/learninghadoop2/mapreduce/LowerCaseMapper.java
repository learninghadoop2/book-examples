package com.learninghadoop2.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LowerCaseMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private Text lowercased = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        lowercased.set(value.toString().toLowerCase());
        context.write(new IntWritable(1), lowercased);
    }
}
