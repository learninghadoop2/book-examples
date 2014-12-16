package com.learninghadoop2.crunch;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Tuple3;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.lib.Join;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class CrunchTermFrequencyInvertedDocumentFrequency extends Configured implements Tool, Serializable {
	private static final long serialVersionUID = 1L;
	
	private Long numDocs;

	public static class TF {
		  String term;
		  String docId;
		  int frequency;

		  public TF() {
			     // Avro reflection needs a zero-arg constructor
		  }

		  public TF(String term, String docId, Integer frequency) {
			  this.term = term;
			  this.docId = docId;
			  this.frequency = (int) frequency;
			  
		  }
	}

	public int run(String[] args) throws Exception {
	    if(args.length != 2) {
	      System.err.println();
	      System.err.println("Usage: " + this.getClass().getName() + " [generic options] input output");

	      return 1;
	    }
	    // Create an object to coordinate pipeline creation and execution.
	    Pipeline pipeline = new MRPipeline(CrunchTermFrequencyInvertedDocumentFrequency.class, getConf());
	    	  
	    // enable debug options
	    pipeline.enableDebug();
	    
	    // Reference a given text file as a collection of Strings.
	    PCollection<String> tweets = pipeline.readTextFile(args[0]);
	    numDocs = tweets.length().getValue();
	    
	    // This is suboptimal. I should calculate it inline down the pipeline.
	    //numDocs = tweets.length();
	    
	    // Use Avro reflections to map the TF POJO to avro schemas */
	    PTable<String, TF> tf = tweets.parallelDo(new TermFrequencyAvro(), Avros.tableOf(Avros.strings(), Avros.reflects(TF.class)));
	    
	    // Calculate DF
	    PTable<String, Long> df = Aggregate.count(tf.parallelDo( new DocumentFrequencyString(), Avros.strings()));
	    
	    
	    // Finally we calculate TF-IDF 
	    PTable<String, Pair<TF, Long>> tfDf = Join.join(tf, df);
	    PCollection<Tuple3<String, String, Double>> tfIdf = tfDf.parallelDo(new TermFrequencyInvertedDocumentFrequency(),
	   		Avros.triples(Avros.strings(), Avros.strings(), Avros.doubles()));

	    // Serialze as avro 
	    tfIdf.write(To.avroFile(args[1]));
	    
	    // Execute the pipeline as a MapReduce.
	    PipelineResult result = pipeline.done();
	    return result.succeeded() ? 0 : 1;
	}

	class TermFrequencyInvertedDocumentFrequency extends MapFn<Pair<String, Pair<TF, Long>>, Tuple3<String, String, Double> >  {
		@Override
		public Tuple3<String, String, Double> map(
				Pair<String, Pair<TF, Long>> input) {

			Pair<TF, Long> tfDf = input.second();
			Long df = tfDf.second();
			
			TF tf = tfDf.first();
			double idf = 1.0 + Math.log(numDocs / df);
			double tfIdf = idf * tf.frequency;
			
			return  Tuple3.of(tf.term, tf.docId, tfIdf);
		}
		
	}
	
	class DocumentFrequencyString extends DoFn<Pair<String, TF>, String> {

			@Override
			public void process(Pair<String, TF> tfAvro,
					Emitter<String> emitter) {
					emitter.emit(tfAvro.first());
			}
	}
	
	/* Multiple outputs per input. Extend a DoFn. */
	class TermFrequencyAvro extends DoFn<String,Pair<String, TF>> {
			private static final long serialVersionUID = 1L;
			/* 
			 * Takes a tweet as input and emits a ((term, docId), frequency) pair.
			 * (term, dociId) is the key, frequency is the value.
			 * 
			 * If we didn't care about the document id we could just emit a Pair<String, Integer> 
			 * representing a (term, frequency) occurrence.
			 *  
			 * */
			public void process(String JSONTweet, Emitter<Pair <String, TF>> emitter) {
                Map<String, Integer> termCount = new HashMap<>();

                String tweet;
                String docId;

                JSONParser parser = new JSONParser();

                try {
                    Object obj = parser.parse(JSONTweet);

                    JSONObject jsonObject = (JSONObject) obj;

                    tweet = (String) jsonObject.get("text");
                    docId = (String) jsonObject.get("id_str");

				
			    	/* count the occurrences of each word in the tweet */
                    for (String term : tweet.split("\\s+")) {
                        if (termCount.containsKey(term.toLowerCase())) {
                            termCount.put(term, termCount.get(term.toLowerCase()) + 1);
                        } else {
                            termCount.put(term.toLowerCase(), 1);
                        }
                    }
				
				    /* Emit term frequency */
                    for (Entry<String, Integer> entry : termCount.entrySet()) {
                        emitter.emit(Pair.of(entry.getKey(), new TF(entry.getKey(), docId, entry.getValue())));
                    }

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
	}

	
	  public static void main(String[] args) throws Exception {
		  ToolRunner.run(new Configuration(), new CrunchTermFrequencyInvertedDocumentFrequency(), args);
	  }
}
