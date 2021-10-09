package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
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
public class MaxHeightKindMapperTest {
    @Mock
    private Mapper.Context context;
    private MaxHeightKindMapper maxHeightKindMapper;

    @Before
    public void setup() {
        this.maxHeightKindMapper = new MaxHeightKindMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "(loc,loc);arr;gen;esp;fam;1985;12.0;cir;adr;nom;var;obj;nev";
        this.maxHeightKindMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("gen"), new DoubleWritable(12.0));
    }
}