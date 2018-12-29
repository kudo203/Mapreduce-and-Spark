package twitter;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

// Find Twitter Social Triangles using Replicated Join (Map only job)
public class FindTriangles extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(FindTriangles.class);

	public static class CountTriangleMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	    //Mapping between follower and followee
		private HashMap<Integer, List<Integer>> follow = new HashMap<Integer, List<Integer>>();

		private enum COUNTER {
            Triangle_Counter
        }

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Integer max = context.getConfiguration().getInt("MAX", Integer.MAX_VALUE);
            try{
                URI[] localPaths = context.getCacheFiles();
                for(URI uri : localPaths){
                    FileSystem fs = FileSystem.get(uri, context.getConfiguration());
                    BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
                    String line;
                    while ((line = rdr.readLine()) != null) {
                        String[] parts = line.split(",");
                        int from = Integer.parseInt(parts[0]);
                        int to = Integer.parseInt(parts[1]);
                        if(from < max && to < max){
                            List<Integer> outEdges;
                            if(follow.containsKey(from)){
                                outEdges = follow.get(from);
                            }
                            else{
                                outEdges = new ArrayList<>();
                            }
                            outEdges.add(to);
                            follow.put(from, outEdges);
                        }
                    }
                }
            }
            catch (IOException e){
                throw new RuntimeException(e);
            }
        }

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            // edges contains the id of the "follower" followed by the id
            // of the follower
            String link[] = value.toString().split(",");
            Integer max = context.getConfiguration().getInt("MAX", Integer.MAX_VALUE);

            int from = Integer.parseInt(link[0]);
            int to = Integer.parseInt(link[1]);

            if(from < max && to < max){
                List<Integer> followee = follow.get(to);
                if(followee != null){
                    for(Integer p : followee){
                        List<Integer> last = follow.get(p);
                        if(last != null && last.contains(from)){
                            context.getCounter(COUNTER.Triangle_Counter).increment(1);
                        }
                    }
                }
            }

		}
	}


	@Override
	public int run(final String[] args) throws Exception {

	    //Configuration
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Count Triangles");
		job.setJarByClass(FindTriangles.class);
		final Configuration jobConf = job.getConfiguration();

		// Sets output delimeter for each line
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

		// Restrict twitter ids
        jobConf.setInt("MAX", Integer.parseInt(args[2]));

		job.setMapperClass(CountTriangleMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// FileInputFormat takes up TextInputFormat as default
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Cache File for Replicated Join
		job.addCacheFile(new Path(args[0] + "/edges.csv").toUri());
        return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> <max>");
		}

		try {
			ToolRunner.run(new FindTriangles(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}