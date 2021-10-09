package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SortHeightReducerTest {
    @Mock
    private Reducer.Context context;
    private SortHeightReducer sortHeightReducer;

    @Before
    public void setup() {
        this.sortHeightReducer = new SortHeightReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Double key = 12.0;
        IntWritable value = new IntWritable(1);
        Iterable<IntWritable> values = Arrays.asList(value, value, value);
        this.sortHeightReducer.reduce(new DoubleWritable(key), values, this.context);
        verify(this.context).write(new DoubleWritable(key), new IntWritable(1));
    }
}
