package com.learninghadoop2.hive.udf;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.io.IOException;
import org.apache.hadoop.io.Text;

public class StringToInt extends UDF {
    public Integer evaluate(Text input) {
        if (input == null)
            return null;

         String str = input.toString();
         return str.hashCode();
    }
}

