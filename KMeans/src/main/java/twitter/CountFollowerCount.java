package twitter;

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

import java.io.IOException;

/* Count follower count for each twitter id
   This ignores those ids with zero followers
 */
public class CountFollowerCount extends Configured implements Tool {


    private static final Logger logger = LogManager.getLogger(CountFollowerCount.class);

    public static class Mapper1 extends Mapper<Object, Text, IntWritable, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] splitLine = value.toString().split(",");
            context.write(new IntWritable(Integer.parseInt(splitLine[1])), new IntWritable(Integer.parseInt(splitLine[0])));
        }
    }

    public static class Reducer1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int count = 0;
            for(IntWritable val : values){
                count+=1;
            }
            context.write(key, new IntWritable(count));
        }
    }

    public int run(final String[] args) throws Exception{
        // Configuration
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Count Follower Count");
        job.setJarByClass(CountFollowerCount.class);
        final Configuration jobConf = job.getConfiguration();
        // Sets output delimeter for each line
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // FileInputFormat takes up TextInputFormat as default
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new CountFollowerCount(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
