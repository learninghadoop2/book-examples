package com.learninghadoop2.crunch;

import java.io.Serializable;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;

/*
 * Basic cleanup pipeline - lowercase and filter stopwords
 */

public class DataPreparationPipeline extends Configured implements Tool, Serializable {
	public int run(String[] args) throws Exception {
	    if(args.length != 2) {
	      System.err.println();
	      System.err.println("Usage: " + this.getClass().getName() + " [generic options] input output");

	      return 1;
	    }
	    // Create an object to coordinate pipeline creation and execution.
	    Pipeline pipeline = new MRPipeline(DataPreparationPipeline.class, getConf());
	    	    
	    // enable debug options
	    pipeline.enableDebug();
	    
	    // Reference a given text file as a collection of Strings.
	    PCollection<String> lines = pipeline.readTextFile(args[0]);

	    // Define a function that splits each line in a PCollection of Strings into a
	    // PCollection made up of the individual words in the file.
	    PCollection<String> words = lines.parallelDo(new Tokenize(), Writables.strings()); 
	    // Remove stopwords
	    PCollection<String> validWords = words.filter(new StopWordFilter());
	  
	    // Execute the pipeline as a MapReduce.
	    pipeline.done();
	    return 0;
	  }
	
		class Tokenize extends DoFn<String, String> {

			@Override
			public void process(String line, Emitter<String> emitter) {
		        for (String word : line.split("\\s+")) {
		        	// emit lower cased words
		          emitter.emit(word.toLowerCase());
		        }
		      }
		}
		
		
		/* Count co-occurrencies of words on strings */
		class BiGram extends DoFn<String, Pair<String, String>> {
			@Override
			public void process(String tweet, Emitter<Pair<String, String>> emitter) {
				 String[] words = tweet.split(" ") ;
				
				 Text bigram = new Text();
                 String prev = null;
                 
                 for (String s : words) {
                   if (prev != null) {
                     emitter.emit(Pair.of(prev, s));
                   }
                   
                   prev = s;
                 }
			}	
		}		
	
	  public static void main(String[] args) throws Exception {
		  ToolRunner.run(new Configuration(), new DataPreparationPipeline(), args);
	  }



	
}
