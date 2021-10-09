package com.opstty.mapper;

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
public class NbTreesKindMapperTest {
    @Mock
    private Mapper.Context context;
    private NbTreesKindMapper nbTreesKindMapper;

    @Before
    public void setup() {
        this.nbTreesKindMapper = new NbTreesKindMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "(loc,loc);arr;gen;esp;fam;ann;haut;cir;adr;nom;var;obj;nev";
        this.nbTreesKindMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("gen"), new IntWritable(1));
    }
}