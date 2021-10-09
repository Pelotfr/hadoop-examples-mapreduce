package com.opstty.mapper;

import com.opstty.MyWritable;
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
public class OldestTreeMapperTest {
    @Mock
    private Mapper.Context context;
    private OldestTreeMapper oldestTreeMapper;

    @Before
    public void setup() {
        this.oldestTreeMapper = new OldestTreeMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "(loc,loc);7;gen;esp;fam;1985;12.0;655.0;adr;nom;var;obj;nev";
        this.oldestTreeMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new MyWritable(7,1985), new IntWritable(1));
    }
}