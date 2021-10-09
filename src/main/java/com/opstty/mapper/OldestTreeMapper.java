package com.opstty.mapper;

import com.opstty.MyWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class OldestTreeMapper extends Mapper<Object, Text, MyWritable, IntWritable> {
    private MyWritable infos = new MyWritable();
    private int arrondissement;
    private int date;
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), ";");
        if (!itr.nextToken().equals("GEOPOINT")) {
            arrondissement = Integer.parseInt(itr.nextToken());
            for (int i =0 ; i < 3 ; i++) {
                itr.nextToken();
            }
            String possible_annee = itr.nextToken();
            if (possible_annee.matches("[0-9]{4}$")){
                date = Integer.parseInt(possible_annee);
                infos.setArr(arrondissement);
                infos.setDate(date);
                context.write(infos, one);
            }

        }
    }
}
