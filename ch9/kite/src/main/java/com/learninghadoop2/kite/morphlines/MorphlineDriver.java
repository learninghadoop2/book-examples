package com.learninghadoop2.kite.morphlines;

import java.io.*;

import org.apache.hadoop.io.Text;
import org.kitesdk.morphline.api.*;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;



public class MorphlineDriver {
	 private static final class RecordEmitter implements Command {
	    private final Text line = new Text();

		@Override
		public Command getParent() {
			return null;
		}

		@Override
		public void notify(Record record) {
			
		}

		@Override
		public boolean process(Record record) {
			line.set(record.get("_attachment_body").toString());
			
			System.out.println(line);
			
			return true;
		}
	    }  
	 
	 
   public static void main(String[] args) throws IOException {
	    /* load a morphline conf and set it up */
	    File morphlineFile = new File(args[0]);    /* "/tmp/morphline.conf" */
	    String morphlineId = args[1];  /* "read_tweets" */
	    MorphlineContext morphlineContext = new MorphlineContext.Builder().build();
	    Command morphline = new Compiler()
                .compile(morphlineFile,
                morphlineId, morphlineContext, new RecordEmitter());
    		
	    /* Prepare the morphline for execution
	     * 
	     * Notifications are sent through the communication channel  
	     * */
	    
	    Notifications.notifyBeginTransaction(morphline);
	    
	    /* Note that we are using the local filesystem, not hdfs*/
	    InputStream in = new BufferedInputStream(new FileInputStream(args[2])); /* /tmp/tweets.json */
	    
	    /* fill in a record and pass  it over */
	    Record record = new Record();
	    record.put(Fields.ATTACHMENT_BODY, in); 
	    
	    try {

            Notifications.notifyStartSession(morphline);
            boolean success = morphline.process(record);
            if (!success) {
              System.out.println("Morphline failed to process record: " + record);
            }
        /* Commit the morphline */
	    } catch (RuntimeException e) {
	        Notifications.notifyRollbackTransaction(morphline);
	        morphlineContext.getExceptionHandler().handleException(e, null);
	      }
       finally {
            in.close();
        }
	    
        /* shut it down */
        Notifications.notifyShutdown(morphline);     
    }
}