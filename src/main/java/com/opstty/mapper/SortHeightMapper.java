package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SortHeightMapper extends Mapper<Object, Text, DoubleWritable, IntWritable> {
    private DoubleWritable height = new DoubleWritable();
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), ";");
        if (!itr.nextToken().equals("GEOPOINT")) {
            for (int i =0 ; i < 4 ; i++) {
                itr.nextToken();
            }
            String possible_annee = itr.nextToken();
            if (possible_annee.matches("[0-9]{4}$")){
                String possible_height = itr.nextToken();
                if(possible_height.matches("[0-9]{1,2}\\.[0-9]")){
                    height.set(Double.parseDouble(possible_height));
                    context.write(height, one);
                }
            }
            else {
                height.set(Double.parseDouble(possible_annee));
                context.write(height, one);
            }
        }
    }
}
