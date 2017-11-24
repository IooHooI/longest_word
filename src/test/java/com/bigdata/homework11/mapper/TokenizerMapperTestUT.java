package com.bigdata.homework11.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class TokenizerMapperTestUT {
    private MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;

    @Before
    public void before() {
        TokenizerMapper tokenizerMapper = new TokenizerMapper();
        mapDriver = MapDriver.newMapDriver(tokenizerMapper);
    }

    @Test
    public void testTokenizerMapper_Case_1() {
        mapDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
        mapDriver.withOutput(new IntWritable(1), new Text("cat"));
        mapDriver.withOutput(new IntWritable(1), new Text("cat"));
        mapDriver.withOutput(new IntWritable(1), new Text("dog"));
        mapDriver.runTest();
    }
}
