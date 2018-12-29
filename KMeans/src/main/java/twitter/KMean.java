package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.*;

public class KMean extends Configured implements Tool {

    enum converge {
        sse
    }

    private static final Logger logger = LogManager.getLogger(KMean.class);

    public static class Mapper1 extends Mapper<Object, Text, DoubleWritable, DoubleWritable> {

        private Set<Double> centroids = new TreeSet<Double>();
        private DoubleWritable mkey = new DoubleWritable();
        private DoubleWritable mvalue = new DoubleWritable();

        // Sets up centroids
        public void setup(Context context) throws IOException, InterruptedException{
            Configuration configuration = context.getConfiguration();

            Integer K = configuration.getInt("K", 0);
            Integer iterationCount = configuration.getInt("IterationCount", 0);

            Map<Double, Double> interimCentroid = new TreeMap<Double, Double>();
            // For first iteration
            if(iterationCount == 0){
                String[] initialCentroids = configuration.get("initialCentroid").split(",");
                for(String cen : initialCentroids){
                    centroids.add(Double.parseDouble(cen));
                }
            }

            // For the following iterations
            else{
                URI[] centroidFiles = context.getCacheFiles();
                if (centroidFiles == null || centroidFiles.length == 0)
                    throw new RuntimeException("Centroids not in distributed cache");

                for(int i = 0; i < centroidFiles.length; i++){
                    FileSystem centroidFs = FileSystem.get(centroidFiles[i], context.getConfiguration());
                    BufferedReader br = new BufferedReader(new InputStreamReader(centroidFs.open(new Path(centroidFiles[i]))));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] lineSplit = line.split(",");
                        Double centroid = Double.parseDouble(lineSplit[0]);
                        Double sse = Double.parseDouble(lineSplit[1]);
                        interimCentroid.put(-sse, centroid);
                        centroids.add(centroid);
                    }
                }
                // In case when centroids are duplicated
                if(centroids.size() != K){
                    Collection c = interimCentroid.values();
                    Iterator iter = c.iterator();
                    Double maxSSECentroid = (Double)iter.next();
                    Double[] nearCentr = nearCentroids(maxSSECentroid, centroids.toArray(new Double[centroids.size()]));
                    Double newCentroid1 = (nearCentr[0] + maxSSECentroid)/2;
                    Double newCentroid2 = (nearCentr[1] + maxSSECentroid)/2;
                    centroids.remove(maxSSECentroid);
                    centroids.add(newCentroid1);
                    centroids.add(newCentroid2);
                }
            }
        }

        // Finds the left nearest and right nearest centroids
        private Double[] nearCentroids(Double target, Double[] a){
            for(int i = 0; i < a.length; i++){
                if(target.equals(a[i])){
                    if(i == 0){
                        return new Double[]{a[0],a[1]};
                    }
                    else if(i == a.length-1){
                        return new Double[]{a[i-1],a[i]};
                    }
                    else{
                        return new Double[]{a[i-1],a[i+1]};
                    }
                }
            }
            return new Double[2];
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] split = value.toString().split(",");
            Long followersCount = Long.parseLong(split[1]);

            double nearestCentroid = centroids.iterator().next();
            double minDistance = Math.abs(followersCount - nearestCentroid);

            for(Double centr : centroids){
                double currentDiff = Math.abs(centr - followersCount);
                if(currentDiff < minDistance){
                    minDistance = currentDiff;
                    nearestCentroid = centr;
                }
            }
            mkey.set(nearestCentroid);
            mvalue.set(followersCount);
            context.write(mkey,mvalue);
        }
    }

    public static class Reducer1 extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable> {

        private DoubleWritable rKey = new DoubleWritable();
        private DoubleWritable rValue = new DoubleWritable();

        public void reduce(DoubleWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{

            Double sum = 0.0;
            Double sse = 0.0;

            int count = 0;

            for(DoubleWritable val : values){
                sum += val.get();
                sse += Math.pow(val.get()-key.get(),2.0);
                count+=1;
            }
            sse = sse/count;
            Double newCentroid = sum/count;
            rKey.set(newCentroid);
            rValue.set(sse);
            context.getCounter(KMean.converge.sse).increment(sse.longValue());
            context.write(rKey, rValue);
        }
    }

    public int run(final String[] args) throws Exception{
        final Configuration conf = getConf();
        int iteration = 0;
        int code = 0;
        String centroidInput, centroidOutput;
        int max = 564512;
        int K = Integer.parseInt(args[2]);
        Long currSSE = 10L;
        Long prevSSE = 100L;

        while(Math.abs(currSSE - prevSSE) > 7 && iteration <= 10){
            centroidInput = iteration == 0 ? args[0] : args[1] + "-" + (iteration - 1);
            centroidOutput = args[1] + "-" + iteration;

            Job job = Job.getInstance(conf, "K Means");
            job.setJarByClass(KMean.class);

            Configuration jobConf = job.getConfiguration();
            jobConf.set("mapreduce.output.textoutputformat.separator", ",");
            jobConf.setInt("K", K);
            jobConf.setInt("MAXFollower", max);
            jobConf.setInt("IterationCount", iteration);

            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(centroidOutput));

            // Sets up distributed cache
            if(iteration != 0){
                Path centroidPath = new Path(centroidInput);

                FileSystem fs = FileSystem.get(centroidPath.toUri(), conf);

                RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(centroidPath, false);

                while (fileIterator.hasNext()) {
                    URI uri = fileIterator.next().getPath().toUri();
                    job.addCacheFile(uri);
                }
            }
            else{
                StringBuilder sb = new StringBuilder();
                Random r = new Random();
                Double rangeMin = 0.0;
                Double increment = (max+0.0)/4;
                Double rangeMax = (max+0.0)/4;
                for(int i = 0; i < K; i++){
                    sb.append((rangeMin + (rangeMax - rangeMin) * r.nextDouble()) + ",");
                    rangeMin += increment;
                    rangeMax += increment;
                }
                /*Random r = new Random();
                Double init = max*r.nextDouble();
                for(int i = 0; i < K; i++){
                    sb.append((init+i) + ",");
                }*/
                jobConf.set("initialCentroid", sb.toString());
            }
            code = job.waitForCompletion(true) ? 0 : 1;
            prevSSE = currSSE;
            currSSE = job.getCounters().findCounter(KMean.converge.sse).getValue();
            iteration+=1;
        }
        return code;

    }

    public static void main(String[] args) {
        if(args.length != 3){
            throw new Error("Four arguments required:\n<data-input> <centroid-output> <K>");
        }
        try{
            ToolRunner.run(new KMean(), args);
        }
        catch (final Exception ex){
            logger.error("", ex);
        }
    }
}
