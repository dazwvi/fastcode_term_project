import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Text;

public class RankMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] links = value.split("\\s+", 2);
        String link = links[0];
        String[] values = links[1].split(",");
        float rank = Float.parseFloat(values[0]);
        String[] outLinks = Arrays.copyOfRange(values, 1, values.length);
        
        for (String outLink : outLinks) {
            float outRank = rank / outLinks.length;
            context.write(new Text(outLink), new Text(Float.toString(outRank)));
        }

        context.write(new Text(link), new Text("|" + String.join(",", outLinks)));
    }
}
