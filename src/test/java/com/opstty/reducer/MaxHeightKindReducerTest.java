package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
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
public class MaxHeightKindReducerTest {
    @Mock
    private Reducer.Context context;
    private MaxHeightKindReducer maxHeightKindReducer;

    @Before
    public void setup() {
        this.maxHeightKindReducer = new MaxHeightKindReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        Iterable<DoubleWritable> values = Arrays.asList(new DoubleWritable(12.0),
                new DoubleWritable(25.0), new DoubleWritable(0.0));
        this.maxHeightKindReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), new DoubleWritable(25.0));
    }
}
