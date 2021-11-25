import java.io.IOException;
import java.util.*;
import static java.lang.Math.min;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.FileSystem;


public class ParallelDijkstra {
    private static enum ReachCounter {COUNT};

    public static class PDMapper
            extends Mapper<LongWritable, Text, IntWritable, MapWritable>{            
            
            private final Text isNode = new Text("isNode");
            private final Text node = new Text("node");
            private final Text dist = new Text("distance");
            private final Text prevNodeID = new Text("prevNodeID");
            private final IntWritable one = new IntWritable(1);
            private final IntWritable zero = new IntWritable(0);
            
            public void map(LongWritable key, Text nodeString, Context context
                    ) throws IOException, InterruptedException {
                PDNodeWritable nodeStructure = new PDNodeWritable();
                IntWritable nodeID = new IntWritable();
                int distance;
                MapWritable unionType1 = new MapWritable();
                MapWritable unionType2 = new MapWritable();

                Configuration conf = context.getConfiguration();
                String option = conf.get("option");
                
                nodeStructure.fromString(nodeString.toString());

                nodeID.set(nodeStructure.getNodeID());
                distance = nodeStructure.getDistance();
                
                unionType1.put(isNode, one);
                unionType1.put(node, nodeStructure);
                context.write(nodeID, unionType1);
                if (distance != Integer.MAX_VALUE) {
                    for(Map.Entry neighbour: nodeStructure.getAdjList().entrySet()) {
                        int myKey;
                        int val;
                        myKey = ((IntWritable) neighbour.getKey()).get();
                        val = ((IntWritable) neighbour.getValue()).get();
                        

                        unionType2.put(isNode, zero);
                        if (option.equals("weighted")) {
                            unionType2.put(dist, new IntWritable(distance + val));
                        }
                        else {
                            unionType2.put(dist, new IntWritable(distance + 1));
                        }
                        unionType2.put(prevNodeID, new IntWritable(nodeID.get()));
                        context.write(new IntWritable(myKey), unionType2);
                    }
                }
            }
    }

    public static class PDReducer
            extends Reducer<IntWritable,MapWritable,IntWritable,Text> {            
            private final Text isNode = new Text("isNode");
            private final Text node = new Text("node");
            private final Text distance = new Text("distance");
            private final Text prev = new Text("prevNodeID");
            
            public void reduce(IntWritable nodeID, Iterable<MapWritable> values, Context context
                    ) throws IOException, InterruptedException {
                PDNodeWritable M = null;
                int prevNodeID = 0;
                int dis;
                int d_min = Integer.MAX_VALUE;
                for(MapWritable element : values) {
                    if (((IntWritable) element.get(isNode)).get() == 1) {
                        M = (PDNodeWritable) element.get(node);
                    }
                    else {
                        dis = ((IntWritable) element.get(distance)).get();
                        if (dis < d_min) {
                            d_min = dis;
                            prevNodeID = ((IntWritable) element.get(prev)).get();
                        }
                    }
                }

                if (d_min < M.getDistance()) {
                    M.setDistance(d_min);
                    M.setPrevNodeID(prevNodeID);
                    // System.out.println("hello there incrementing COUNT");
                    context.getCounter(ReachCounter.COUNT).increment(1);
                }
                
                context.write(nodeID, new Text(M.toString()));
            }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        // setup local mode for debugging
        // conf.set("fs.defaultFS", "file:///");
        // conf.set("mapreduce.framework.name", "local");

        // read arguments from command-line
        int numIterations = Integer.parseInt(args[3]);
        conf.set("sourceNodeID", args[2]);
        conf.set("option", args[4]);
        
        // setup first job for pre-processing input edge data
        Job job0 = Job.getInstance(conf, "pre-processing");
        job0.setJarByClass(PDPreProcess.class);
        job0.setMapperClass(PDPreProcess.PreMapper.class);
        job0.setReducerClass(PDPreProcess.PreReducer.class);
        job0.setMapOutputKeyClass(IntWritable.class);
        job0.setMapOutputValueClass(MapWritable.class);
		job0.setOutputKeyClass(IntWritable.class);
		job0.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job0, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job0, new Path("/output0"));
        FileOutputFormat.setOutputPath(job0, new Path("/output0"));

        job0.waitForCompletion(true);
        
        int i = 0;
        if (numIterations == 0) {
            // run till convergence
            long reachCount = -1;
            do {
                Job job = Job.getInstance(conf, "parallel dijkstra iteration");
                job.setJarByClass(ParallelDijkstra.class);
                job.setMapperClass(ParallelDijkstra.PDMapper.class);
                // job.setCombinerClass(IntSumReducer.class);
                job.setReducerClass(ParallelDijkstra.PDReducer.class);
                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(MapWritable.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(PDNodeWritable.class);

                FileInputFormat.addInputPath(job, new Path("/output" + i + "/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("/output" + (i + 1)));
                job.waitForCompletion(true);

                reachCount = job.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();
                i++;
            } while (reachCount != 0);
        }
        else {
            long reachCount = -1;
            for (i = 0; i < numIterations; i++) {

                Job job = Job.getInstance(conf, "parallel dijkstra iteration");
                job.setJarByClass(ParallelDijkstra.class);
                job.setMapperClass(ParallelDijkstra.PDMapper.class);
                // job.setCombinerClass(IntSumReducer.class);
                job.setReducerClass(ParallelDijkstra.PDReducer.class);
                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(MapWritable.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(PDNodeWritable.class);

                FileInputFormat.addInputPath(job, new Path("/output" + i + "/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("/output" + (i+1)));
                job.waitForCompletion(true);

                
                reachCount = job.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();
                
                if (reachCount == 0 ) {
                    System.out.println("breaking out early in iteration: " + (i+1));
                    i++;
                    break;
                }
            }
        }

        Job jobFinal = Job.getInstance(conf, "post-processing");
        jobFinal.setJarByClass(PDPostProcess.class);
        jobFinal.setMapperClass(PDPostProcess.PostMapper.class);
        jobFinal.setReducerClass(PDPostProcess.PostReducer.class);
        jobFinal.setMapOutputKeyClass(IntWritable.class);
        jobFinal.setMapOutputValueClass(Text.class);
		jobFinal.setOutputKeyClass(IntWritable.class);
		jobFinal.setOutputValueClass(Text.class);
        jobFinal.setNumReduceTasks(0);

        FileInputFormat.addInputPath(jobFinal, new Path("/output" + i + "/part-r-00000"));
        FileOutputFormat.setOutputPath(jobFinal, new Path(args[1]));

        System.exit(jobFinal.waitForCompletion(true) ? 0 : 1);

    }
}
