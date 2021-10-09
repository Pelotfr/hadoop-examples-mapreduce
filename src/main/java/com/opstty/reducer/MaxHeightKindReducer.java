package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxHeightKindReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        Double max = 0.0;
        for (DoubleWritable val : values) {
            if (val.get() > max){
                max = val.get();
            }
        }
        result.set(max);
        context.write(key, result);
    }
}
