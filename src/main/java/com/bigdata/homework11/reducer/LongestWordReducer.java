package com.bigdata.homework11.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LongestWordReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    protected void reduce(IntWritable key, Iterable<Text> value, Context context) throws java.io.IOException, InterruptedException {
        String longestWord = "";
        StringBuilder templongestWordsList = new StringBuilder();
        for (Text word : value) {
            if (word.toString().length() > longestWord.length()) {
                longestWord = word.toString();
                templongestWordsList.setLength(0);
                templongestWordsList.append(longestWord);
                templongestWordsList.append(" ");
            } else if (word.toString().length() == longestWord.length()) {
                if (templongestWordsList.indexOf(word.toString()) == -1) {
                    templongestWordsList.append(word.toString());
                    templongestWordsList.append(" ");
                }
            }
        }
        context.write(new Text("bla"), new IntWritable(1));
    }
}
