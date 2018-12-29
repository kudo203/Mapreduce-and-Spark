package twitter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;


public class AdjacencyListGenerator extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(AdjacencyListGenerator.class);

    public static class Mapper1 extends Mapper<Object, Text, Text, Text>{

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] link = value.toString().split(",");
            context.write(new Text(link[0]), new Text(link[1]));
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text>{

        @Override
        public void reduce(Text vertex, Iterable<Text> neighbors, Context context) throws IOException, InterruptedException {
            StringBuilder adjacencyList = new StringBuilder();

            for (Text neighbor : neighbors) {
                adjacencyList.append(neighbor);
                adjacencyList.append(",");
            }
            if(adjacencyList.length() > 0){
                context.write(vertex, new Text(adjacencyList.substring(0, adjacencyList.length() - 1)));
            }
            else{
                context.write(vertex, new Text(""));
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception{
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Generate Adjacency List");
        job.setJarByClass(AdjacencyListGenerator.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ":");

        job.setMapperClass(Mapper1.class);

        job.setCombinerClass(Reducer1.class);
        job.setReducerClass(Reducer1.class);
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
            ToolRunner.run(new AdjacencyListGenerator(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
