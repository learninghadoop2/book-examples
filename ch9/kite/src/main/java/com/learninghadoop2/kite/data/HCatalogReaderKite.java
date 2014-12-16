package com.learninghadoop2.kite.data;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HCatalogReaderKite extends Configured implements Tool {
	  @Override
	  public int run(String[] args) throws Exception {
	    Dataset<GenericRecord> tweets = Datasets.load("repo:hive/tweets_hcat");

	    DatasetReader<GenericRecord> reader = tweets.newReader();
	    try {
	      for (GenericRecord tweet : reader) {
	        System.out.println(tweet);
	      }
	    } finally {
	      reader.close();
	    }

	    return 0;
	  }

	  public static void main(String... args) throws Exception {
	    int rc = ToolRunner.run(new HCatalogReaderKite(), args);
	    System.exit(rc);
	  }
}
