package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MaxHeightKindMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text kind = new Text();
    private DoubleWritable height = new DoubleWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), ";");
        if (!itr.nextToken().equals("GEOPOINT")) {
            for (int i =0 ; i < 1 ; i++) {
                itr.nextToken();
            }
            kind.set(itr.nextToken());
            for (int i =0 ; i < 2; i++) {
                itr.nextToken();
            }
            String possible_annee = itr.nextToken();
            if (possible_annee.matches("[0-9]{4}$")){
                String possible_height = itr.nextToken();
                if(possible_height.matches("[0-9]{1,2}\\.[0-9]")){
                    height.set(Double.parseDouble(possible_height));
                    context.write(kind, height);
                }
            }
            else {
                height.set(Double.parseDouble(possible_annee));
                context.write(kind, height);
            }
        }
    }
}
