package com.learninghadoop2.pig.udf;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class StringToInt extends EvalFunc<Integer> {
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {
            String str = (String) input.get(0);
            return str.hashCode();
        } catch(Exception e) {
            throw new IOException("Cannot convert String to Int", e);
        }
    }
}

