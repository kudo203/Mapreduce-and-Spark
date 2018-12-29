package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ShortestPaths extends Configured implements Tool {

    enum needIteration {
        ifUpdated
    }

    private static final Logger logger = LogManager.getLogger(ShortestPaths.class);


    /* Mapper class to map each node to their adjacency list and
       probable new shortest path parent
    */
    public static class Mapper1 extends Mapper<Object, Text, Text, Text>{

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String source = context.getConfiguration().get("source");
            String[] splitLine = value.toString().split(":");
            String zeroString = Long.toString(0);
            String inf = Long.toString(Long.MAX_VALUE);

            if(splitLine.length >= 2){
                String from = splitLine[0];
                String neighbors = splitLine[1];
                long distance = Long.MAX_VALUE;

                // Case handing first iteration
                if(splitLine.length == 2){
                    if(from.equals(source)){
                        distance = 0;
                        context.write(new Text(from), new Text(String.join(":", splitLine[1], zeroString)));
                    }
                    else{
                        context.write(new Text(from), new Text(String.join(":", splitLine[1], inf)));
                    }
                }
                // Case handling iteration > 1
                else{
                    distance = Long.parseLong(splitLine[2]);
                    context.write(new Text(from), new Text(String.join(":", splitLine[1], splitLine[2])));
                }

                String[] neigborSplit = neighbors.split(",");
                if(neigborSplit.length == 1 && neigborSplit[0].length() == 0)
                    return;

                // Emit possibly new shortest distance for each neighbor
                for(String neighbor : neigborSplit){
                    long newDis = distance;
                    if(distance != Long.MAX_VALUE){
                        newDis += 1;
                    }
                    context.write(new Text(neighbor), new Text(Long.toString(newDis)));
                }
            }
        }
    }

    /*
       Updates the shortest distance to the node
    */
    public static class Reducer1 extends Reducer<Text, Text, Text, Text>{

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            long minDis = Long.MAX_VALUE;
            long currentDistance = Long.MAX_VALUE;
            Counter updated = context.getCounter(needIteration.ifUpdated);
            String neigbors = "";

            for(Text value : values){
                String[] valueSplit = value.toString().split(":");
                // Case for adjacency list
                if(valueSplit.length == 1){
                    long distance = Long.parseLong(valueSplit[0]);
                    if(distance < minDis){
                        minDis = distance;
                    }
                }
                // Case for new shortest distance
                else{
                    currentDistance = Long.parseLong(valueSplit[1]);
                    if(currentDistance < minDis)
                        minDis = currentDistance;
                    neigbors = valueSplit[0];
                }
            }

            if(minDis < currentDistance){
                updated.increment(1);
            }

            context.write(key, new Text(String.join(":", neigbors, Long.toString(Math.min(minDis, currentDistance)))));

        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        // Configuration
        final Configuration conf = getConf();
        // single source
        conf.set("source", args[0]);

        long ifUpdated = 1;
        int code = 0;
        int iterationCount = 1;
        FileSystem hdfs = FileSystem.get(conf);

        // BFS Loop
        while(ifUpdated > 0){
            Job job = Job.getInstance(conf, "Shortest Path Iteration");
            job.setJarByClass(ShortestPaths.class);
            Configuration jobConf = job.getConfiguration();
            jobConf.set("mapreduce.output.textoutputformat.separator", ":");

            String input, output;

            if(iterationCount == 1){
                input = args[1];
            }
            else{
                input = args[2] + "-" + (iterationCount-1);
            }
            output = args[2] + "-" + iterationCount;

            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));

            code = job.waitForCompletion(true) ? 0 : 1;

            Counters jobCounters = job.getCounters();
            ifUpdated = jobCounters.findCounter(needIteration.ifUpdated).getValue();
            if (iterationCount > 1) {
                hdfs.delete(new Path(input), true);
            }
            iterationCount++;
        }

        return code;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir> <source>");
        }

        try {
            ToolRunner.run(new ShortestPaths(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
