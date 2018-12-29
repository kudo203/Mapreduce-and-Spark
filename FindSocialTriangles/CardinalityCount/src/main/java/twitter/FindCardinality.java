package twitter;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class FindCardinality extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(FindCardinality.class);

	/*
	Mapper which maps a line of following info to
	to Source Node and Sink Node
	 */
	public static class InOutDegreeMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Text outValue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String link[] = value.toString().split(",");
            int follower = Integer.parseInt(link[0]);
            int followee = Integer.parseInt(link[1]);

            outValue.set("S");
            context.write(new IntWritable(follower), outValue);

            outValue.set("T");
            context.write(new IntWritable(followee), outValue);
		}
	}

	/*
    Calculates cardinality
	 */
	public static class CountCardinalityReducer extends Reducer<IntWritable, Text, Text, Text> {
        private enum COUNTER {
            cardinality
        }

		@Override
		public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            long indegree = 0;
            long outdegree = 0;

            for(Text val : values){
                if(val.charAt(0) == 'S')
                    outdegree += 1;
                else
                    indegree += 1;
            }

            long keyCardinality = (indegree * outdegree);
            context.getCounter(COUNTER.cardinality).increment(keyCardinality);
            context.write(null, null);
        }
	}

	@Override
	public int run(final String[] args) throws Exception {

		//Configuration
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Count Cardinality");
		job.setJarByClass(FindCardinality.class);
		final Configuration jobConf = job.getConfiguration();

		// Sets output delimeter for each line
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		job.setMapperClass(InOutDegreeMapper.class);
		job.setReducerClass(CountCardinalityReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// FileInputFormat takes up TextInputFormat as default
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new FindCardinality(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}