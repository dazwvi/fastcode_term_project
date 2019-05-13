
package mapred.pagerank;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class LinkMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {
        String[] pair = value.toString().split("\\s+");
        String from = pair[0];
        String to = pair[1];

        context.write(new Text(from), new Text(to));
    }
}
