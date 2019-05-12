package mapred.pagerank;

import java.io.IOException;
import java.util.HashMap;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class PageMapper extends Mapper<LongWritable, Text, Text, Text> {
	HashMap urls;

	@Override
	protected void setup(Context context) throws IOException
	{
		urls = new HashMap<String, String>();
		
		Path path = new Path(context.getConfiguration().get("urls_path"));
		FileSystem fs = path.getFileSystem(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

		String line;
		while ((line = br.readLine()) != null)
		{
			String[] split = line.split(" ");
			urls.put(split[0], split[1]);
		}
		
		br.close();
	}

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {
		String[] value_list = value.toString().split("\\s", 2);

		String url = urls.get(value_list[0]).toString();
		String rank = value_list[1].split(",")[0];
		context.write(new Text(rank), new Text(url));
    }
}