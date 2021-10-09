package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SortHeightMapperTest {
    @Mock
    private Mapper.Context context;
    private SortHeightMapper sortHeightMapper;

    @Before
    public void setup() {
        this.sortHeightMapper = new SortHeightMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "(loc,loc);arr;gen;esp;fam;1985;12.0;655.0;adr;nom;var;obj;nev";
        this.sortHeightMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new DoubleWritable(12.0), new IntWritable(1));
    }
}