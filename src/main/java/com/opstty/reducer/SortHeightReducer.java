package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortHeightReducer extends Reducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable> {
    public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
