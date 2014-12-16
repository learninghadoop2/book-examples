package com.learninghadoop2.crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.Serializable;

public class HashtagCount extends Configured implements Tool, Serializable {
  public int run(String[] args) throws Exception {
    Pipeline pipeline = new MRPipeline(HashtagCount.class, getConf());
    
    // enable debug options
    pipeline.enableDebug();
    
    PCollection<String> lines = pipeline.readTextFile(args[0]);

    PCollection<String> words = lines.parallelDo(new DoFn<String, String>() {
      public void process(String line, Emitter<String> emitter) {
        for (String word : line.split("\\s+")) {
        	if (word.matches("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)")) {
        		emitter.emit(word);
        	}
        }
      }
    }, Writables.strings()); // Indicates the serialization format

    PTable<String, Long> counts = words.count();

    pipeline.writeTextFile(counts, args[1]);

    pipeline.done();
    return 0;
  }

  public static void main(String[] args) throws Exception {
	  ToolRunner.run(new Configuration(), new HashtagCount(), args);
  }
}
