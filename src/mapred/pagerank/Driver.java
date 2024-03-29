package mapred.pagerank;

import java.util.HashSet;
import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.FileUtil;
import mapred.util.SimpleParser;
import mapred.pagerank.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Driver {
	private final static int iterations = 30;
	public static HashSet<String> nodeSet = new HashSet<String>();

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");
		String url_input = parser.get("uinput");

		getLinks(input, tmpdir + "/iter-0");

		//Read/Write files in every iterations.
		for(int i = 0; i < iterations; i++){
			String dir = tmpdir + "/iter-" + (i+1);
			String dirPrev = tmpdir + "/iter-" + i;
			if (i == iterations - 1){
				// getRanks(dirPrev, tmpdir + "/last_iter");
				getRanks(dirPrev, output);
			}
			else{
				getRanks(dirPrev, dir);
			}
		}

		// getPages(tmpdir + "/last_iter", output, url_input);

	}

	/**
	 * 
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static void getLinks(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get all outlinks");

		job.setClasses(LinkMapper.class, LinkReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}

	/**
	 * @param input
	 * @param output
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getRanks(
			String input, 
			String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Calculate PageRank");
		job.setClasses(RankMapper.class, RankReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();
	}

	/**
	 * @param input
	 * @param output
	 * @param urls
	 * @throws Exception
	 */
	private static void getPages(String input, String output, String urls) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("urls_path", urls);

		Optimizedjob job = new Optimizedjob(conf, input, output,
				"Get Page Urls");
		job.setClasses(PageMapper.class, null, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();
	}
}
