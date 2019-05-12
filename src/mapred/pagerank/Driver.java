package mapred.hashtagsim;

import java.util.HashSet;
import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.FileUtil;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Driver {
	private final static int iterations = 30;
	public static HashSet<String> nodeSet = new HashSet<String>();
	public final static double damp_factor = 0.85;
	public final static String sinkSign = "|";

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		getLinks(input, tmpdir + "/get_link/output");

		//Read/Write files in every iterations.
		for(int i = 0; i < iterations; i++){
			String dir = tmpdir + "/iter-" + (i+1);
			String dirPrev = tmpdir + "/iter-" + i;
			if (i == iterations - 1){
				getRanks(dir + "/tmp", dirPrev + "/output", output, i);
			}
			else{
				getRanks(dir + "/tmp", dirPrev + "/output", dir + "/output", i);
			}
		}

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
	 * @param temp_dir
	 * @param input
	 * @param output
	 * @param current_iter
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getRanks(
			String temp_dir,
			int current_iter,
			String input, 
			String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("tmpDir", tmpDir);
		
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"Get similarities between #job and all other hashtags");
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
