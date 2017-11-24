package com.bigdata.homework11.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LongestWordReducerTestUT {
    private ReduceDriver<IntWritable, Text, Text, IntWritable> reduceDriver;

    @Before
    public void before() {
        LongestWordReducer longestWordReducer = new LongestWordReducer();
        reduceDriver = ReduceDriver.newReduceDriver(longestWordReducer);
    }

    @Test
    public void test_Longest_Word_Reducer_Case_1() {
        List<Text> values = new ArrayList<>();
        values.add(new Text("cat"));
        values.add(new Text("cow"));
        values.add(new Text("snake"));
        values.add(new Text("chicken"));
        reduceDriver
                .withInput(new IntWritable(1), values)
                .withOutput(new Text("chicken "), new IntWritable(7));
        reduceDriver.runTest();
    }

    @Test
    public void test_Longest_Word_Reducer_Case_2() {
        List<Text> values = new ArrayList<>();
        values.add(new Text("cat"));
        values.add(new Text("cow"));
        values.add(new Text("snake"));
        values.add(new Text("tiger"));
        reduceDriver
                .withInput(new IntWritable(1), values)
                .withOutput(new Text("snake tiger "), new IntWritable(5));
        reduceDriver.runTest();
    }

    @Test
    public void test_Longest_Word_Reducer_Case_2_1() {
        List<Text> values = new ArrayList<>();
        values.add(new Text("snake"));
        values.add(new Text("cat"));
        values.add(new Text("tiger"));
        values.add(new Text("cow"));
        values.add(new Text("snake"));
        values.add(new Text("tiger"));
        values.add(new Text("tiger"));
        reduceDriver
                .withInput(new IntWritable(1), values)
                .withOutput(new Text("snake tiger "), new IntWritable(5));
        reduceDriver.runTest();
    }
}
