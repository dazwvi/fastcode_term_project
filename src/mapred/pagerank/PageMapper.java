package main.pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PageMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	@Override
	protected void setup(Context context) throws IOException
	{
		urls = new HashMap<String, String>();
		
		Path path = new Path(context.getConfiguration().get("urls_path"));
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		
		boolean firstLine = true;
		int currentNode = 0;
		int nodesCount = 0;

		String line;
		while ((line = br.readLine()) != null)
		{
			String[] split = line.split(" ");
			
			if (firstLine)
			{
				nodesCount = Integer.parseInt(split[0]);
				firstLine = false;
			}
			else
			{
				if (currentNode < nodesCount)
				{
					urls.put(split[0], split[1]);
					currentNode++;
				}
				else 
				{
					break;
				}
			}
		}
		
		br.close();
	}

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {
        String[] value_list = value.toString().split("\t");
		
		String url = urls.get(value_list[0]);
		String rank = value_list[1];
		
		context.write(new Text(rank), new Text(url));
    }
}