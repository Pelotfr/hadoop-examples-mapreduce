package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class NbTreesKindMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), ";");
        if (!itr.nextToken().equals("GEOPOINT")) {
            for (int i =0 ; i < 1 ; i++) {
                itr.nextToken();
            }
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
