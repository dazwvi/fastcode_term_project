package main.pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class HashtagMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {
        String[] pair = value.toString().split(" ");
        String from = pair[0];
        String to = pair[1];

        context.write(new IntWritable())
    }
}
