package com.bigdata.homework11;

import com.bigdata.homework11.mapper.TokenizerMapper;
import com.bigdata.homework11.reducer.LongestWordReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LongestWordJob {

    public static void run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
            System.err.println("Usage: LongestWord <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Longest Word");
        job.setJarByClass(LongestWordJob.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(LongestWordReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        Path outputPath = new Path(remainingArgs[1]);
        outputPath.getFileSystem(conf).delete(new Path(remainingArgs[1]),true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}