package com.learninghadoop2.kite.morphlines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

public class MorphlineDriverMapReduce
       extends Configured implements Tool {
    private static final String MORPHLINE_CONF = "morphline.conf";
    private static final String MORPHLINE_ID = "read_tweets";

    public static class ReadTweets
            extends Mapper<Object, Text, Text, NullWritable> {
        private final Record record = new Record();
        private Command morphline;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            File morphlineConf = new File(context.getConfiguration()
                    .get(MORPHLINE_CONF));
            String morphlineId = context.getConfiguration()
                    .get(MORPHLINE_ID);
            MorphlineContext morphlineContext = new MorphlineContext.Builder()
                    .build();

            morphline = new org.kitesdk.morphline.base.Compiler()
                    .compile(morphlineConf,
                            morphlineId,
                            morphlineContext,
                            new RecordEmitter(context));
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            record.put(Fields.ATTACHMENT_BODY, 
        new ByteArrayInputStream(value.toString().getBytes("UTF-8")));
        morphline.process(record);
                    if (!morphline.process(record)) {
              System.out.println(
"Morphline failed to process record: " + record);
        }

            record.removeAll(Fields.ATTACHMENT_BODY);
        }
    }

    private static final class RecordEmitter implements Command {
        private final Text line = new Text();
        private final Mapper.Context context;

        private RecordEmitter(Mapper.Context context) {
            this.context = context;
        }

        @Override
        public void notify(Record notification) {
        }

        @Override
        public Command getParent() {
            return null;
        }

        @Override
        public boolean process(Record record) {
            line.set(record.get(Fields.ATTACHMENT_BODY).toString());
            try {
                context.write(line, null);
            } catch (Exception e) {
                e.printStackTrace();
        return false;
        }
            return true;
        }

    }
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set(MORPHLINE_CONF, args[0]);
        conf.set(MORPHLINE_ID, args[1]);

        Job job = Job.getInstance(conf, "read tweets");
        job.setJarByClass(MorphlineDriverMapReduce.class);
        job.setMapperClass(ReadTweets.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new MorphlineDriverMapReduce(), args);
        System.exit(exitCode);
    }
}