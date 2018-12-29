package twitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

// Finds a three hop path and determines if it's a triangle
public class FindTriangleStep2 extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(FindTriangleStep2.class);

    /*
    Mapper reading from two hop edges dataset
     */
    public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String link[] = value.toString().split(",");
            outkey.set(link[1]);
            outvalue.set("S" + link[0]);
            context.write(outkey, outvalue);
        }
    }

    /*
    * Mapper reading from original dataset
    * */
    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            Integer max = context.getConfiguration().getInt("MAX", Integer.MAX_VALUE);
            String link[] = value.toString().split(",");

            if(Integer.parseInt(link[0]) < max && Integer.parseInt(link[1]) < max ){
                outkey.set(link[0]);
                outvalue.set("T" + link[1]);
                context.write(outkey, outvalue);
            }
        }
    }

    /*
    Generates three hop edges and finds social triangles
     */
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private enum COUNTER {
            Triangle_Counter
        };

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            List<String> source = new ArrayList<String>();
            List<String> sink = new ArrayList<String>();

            for(Text val : values){
                if(val.charAt(0) == 'S')
                    source.add(val.toString().substring(1));
                else
                    sink.add(val.toString().substring(1));
            }
            for(String s : source){
                for(String t : sink){
                    if(s.equals(t))
                        context.getCounter(COUNTER.Triangle_Counter).increment(1);
                }
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        // Configuration
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Count Followers");

        job.setJarByClass(FindTriangleStep2.class);
        final Configuration jobConf = job.getConfiguration();

        // Sets output delimeter for each line
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        jobConf.setInt("MAX", Integer.parseInt(args[3]));

        Path input1 = new Path(args[0]);
        Path input2 = new Path(args[1]);

        MultipleInputs.addInputPath(job, input1, TextInputFormat.class, Mapper1.class);
        MultipleInputs.addInputPath(job, input2, TextInputFormat.class, Mapper2.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(final String[] args) {
        if (args.length != 4) {
            throw new Error("Three arguments required:\n<input-dir> <path2-input-dir> <output-dir> MAX");
        }

        try {
            ToolRunner.run(new FindTriangleStep2(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}