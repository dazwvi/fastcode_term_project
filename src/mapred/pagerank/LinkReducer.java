package mapred.pagerank;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class LinkReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        String outLinks = "1";
        for (Text val : value) {
            outLinks += "," + val.toString();
        }
        context.write(key, new Text(outLinks));
    }
}