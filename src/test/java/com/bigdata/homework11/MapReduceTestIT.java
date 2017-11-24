package com.bigdata.homework11;

import com.bigdata.homework11.mapper.TokenizerMapper;
import com.bigdata.homework11.reducer.LongestWordReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MapReduceTestIT {
    private MapReduceDriver<LongWritable, Text, IntWritable, Text, Text, IntWritable> mapReduceDriver;

    @Before
    public void before() {
        TokenizerMapper tokenizerMapper = new TokenizerMapper();
        LongestWordReducer longestWordReducer = new LongestWordReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(tokenizerMapper, longestWordReducer);
    }

    @Test
    public void test_Map_Reduce_IT_Case_1() {
        mapReduceDriver
                .withInput(new LongWritable(1), new Text("cat cat dog cow tiger"))
                .withOutput(new Text("tiger "), new IntWritable(5));
        mapReduceDriver.runTest();
    }

    @Test
    public void test_Map_Reduce_IT_Case_2() {
        mapReduceDriver
                .withInput(new LongWritable(1), new Text("cat cat dog cow tiger snake"))
                .withOutput(new Text("tiger snake "), new IntWritable(5));
        mapReduceDriver.runTest();
    }

    @Test
    public void test_Map_Reduce_IT_Case_2_1() {
        mapReduceDriver
                .withInput(new LongWritable(1), new Text("cat tiger cat dog cow tiger snake snake snake"))
                .withOutput(new Text("tiger snake "), new IntWritable(5));
        mapReduceDriver.runTest();
    }
}
