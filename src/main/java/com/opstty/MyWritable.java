package com.opstty;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritable implements Writable {
    private int arr;
    private int date;

    public MyWritable(){}

    public MyWritable(int v1, int v2){ set(v1, v2);}

    public void set(int v1, int v2){ setArr(v1); setDate(v2);}

    public int getArr() {
        return arr;
    }

    public void setArr(int arr) {
        this.arr = arr;
    }

    public int getDate() {
        return date;
    }

    public void setDate(int date) {
        this.date = date;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(arr);
        out.writeInt(date);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        arr = in.readInt();
        date = in.readInt();
    }
}
