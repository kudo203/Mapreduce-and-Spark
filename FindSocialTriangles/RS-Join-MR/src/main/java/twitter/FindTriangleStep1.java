package twitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

// Class for two hop paths
public class FindTriangleStep1 extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(FindTriangleStep1.class);

	/*
	Mapper which maps each follow connection into source node and sink nodes
	 */
	public static class Path2EmitMapper extends Mapper<Object, Text, Text, Text> {

	    private Text outkey = new Text();
	    private Text outvalue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            Integer max = context.getConfiguration().getInt("MAX", Integer.MAX_VALUE);
		    String link[] = value.toString().split(",");

		    if(Integer.parseInt(link[0]) < max && Integer.parseInt(link[1]) < max ){
                outkey.set(link[1]);
                outvalue.set("S" + link[0]);
                context.write(outkey, outvalue);

                outkey.set(link[0]);
                outvalue.set("T" + link[1]);
                context.write(outkey, outvalue);
            }
		}
	}

	/*
    Reducer that creates a two hop path by introducing a new edge
    made by two old edges and a node
	 */
	public static class Path2Reducer extends Reducer<Text, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            List<String> source = new ArrayList<String>();
            List<String> sink = new ArrayList<String>();

            // Separating out
            for(Text val : values){
                if(val.charAt(0) == 'S')
                    source.add(val.toString().substring(1));
                else
                    sink.add(val.toString().substring(1));
            }

            for(String s : source){
                outkey.set(s);
                for(String t : sink){
                    outvalue.set(t);
                    context.write(outkey, outvalue);
                }
            }

        }
	}

	@Override
	public int run(final String[] args) throws Exception {

		// Configuration
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Count Triangles");
		job.setJarByClass(FindTriangleStep1.class);
		final Configuration jobConf = job.getConfiguration();

		// Sets output delimeter for each line
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		jobConf.setInt("MAX", Integer.parseInt(args[2]));

		job.setMapperClass(Path2EmitMapper.class);
		job.setReducerClass(Path2Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// FileInputFormat takes up TextInputFormat as default
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> max");
		}

		try {
			ToolRunner.run(new FindTriangleStep1(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}