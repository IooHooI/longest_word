package com.bigdata.homework11.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private String bla = "Jeoppa";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String txt = value.toString();
        String[] lines = txt.split(" ");
        IntWritable key1  = new IntWritable(1);
        Text value1 = new Text();
        for(String line: lines){
            if(line.length() > 0){
                value1.set(line);
                context.write(key1, value1);
            }
        }
    }
}
