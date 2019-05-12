package mapred.pagerank;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        String outLinks = "";
        float rank = 0;
        float factor = 0.85f;
        for (Text v : value) {
            String val = v.toString();
            if (val.startsWith("|")) {
                outLinks = val.substring(1);
            } else {
                rank += Float.parseFloat(val);
            }
        }
        rank = 1 - factor + factor * rank;
        context.write(key, new Text(Float.toString(rank) + "," + outLinks));
    }
}