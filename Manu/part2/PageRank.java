import java.io.IOException;
import java.util.*;
import static java.lang.Math.min;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

    public static class PRMapper
            extends Mapper<LongWritable, Text, IntWritable, MapWritable>{            
            
            private final Text isNode = new Text("isNode");
            private final Text node = new Text("node");
            private final Text rank = new Text("rank");
            private final IntWritable one = new IntWritable(1);
            private final IntWritable zero = new IntWritable(0);
            
            public void map(LongWritable key, Text nodeString, Context context
                    ) throws IOException, InterruptedException {
                    PRNodeWritable PRnode = new PRNodeWritable();
                    Configuration conf = context.getConfiguration();
                    int n=Integer.parseInt(conf.get("nodeNum"));
                    PRnode.fromString(nodeString.toString());
                    if (PRnode.getPageRank()==-1){
                        PRnode.setPageRank((double) 1 / (double) n) ;
                    }
                    IntWritable nodeId = new IntWritable();
                    nodeId.set(PRnode.getNodeID());
                    DoubleWritable pageRank = new DoubleWritable();
                    pageRank.set(PRnode.getPageRank());
                    MapWritable unionType = new MapWritable();
                    unionType.put(isNode, new IntWritable(1));
                    unionType.put(node, (Writable) PRnode);
                    context.write(nodeId, unionType);
                    for(IntWritable v: PRnode.getAdjList()) {
                        unionType.put(isNode, zero);
                        unionType.put(rank, new DoubleWritable(pageRank.get()/(double) PRnode.getAdjList().size()));
                        context.write(v, unionType);
                    }
            }
    }

    public static class PRReducer
            extends Reducer<IntWritable,MapWritable,IntWritable,Text> {            
            private final Text isNode = new Text("isNode");
            private final Text node = new Text("node");
            private final Text rank = new Text("rank");
            
            public void reduce(IntWritable nodeId, Iterable<MapWritable> values, Context context
                    ) throws IOException, InterruptedException {
                PRNodeWritable M = null;
                DoubleWritable pageRank = new DoubleWritable();
                for(MapWritable element : values) {
                    if (((IntWritable) element.get(isNode)).get() == 1) {
                        M = (PRNodeWritable) element.get(node);
                    }
                    else {
                        pageRank = new DoubleWritable(pageRank.get() + ((DoubleWritable) element.get(rank)).get());
                    }
                }
                M.setPageRank(pageRank.get());
                //System.out.println("--Reducer--- " + new Text(M.toString()));
                context.write(new IntWritable(M.getNodeID()), new Text(M.toString()));
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        // setup local mode for debugging
       // conf.set("fs.defaultFS", "file:///");
        //conf.set("mapreduce.framework.name", "local");

        // read arguments from command-line
        int numIterations = Integer.parseInt(args[1]);
        conf.set("alpha", args[0]);
        Job job0 = Job.getInstance(conf, "pre-processing");
        job0.setJarByClass(PRPreProcess.class);
        job0.setMapperClass(PRPreProcess.PreMapper.class);
        job0.setReducerClass(PRPreProcess.PreReducer.class);
        job0.setMapOutputKeyClass(IntWritable.class);
        job0.setMapOutputValueClass(IntWritable.class);
		job0.setOutputKeyClass(IntWritable.class);
		job0.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job0, new Path(args[2]));
        // FileOutputFormat.setOutputPath(job0, new Path("/output0"));
        FileOutputFormat.setOutputPath(job0, new Path("/a2temp/output0"));
        job0.waitForCompletion(true);
        long cnt = job0.getCounters().findCounter(PRPreProcess.num.COUNT).getValue();
        conf.set("nodeNum", String.valueOf(cnt));
        for (int i=0; i<numIterations;i++){
                //System.out.println("----------1------------->" + i + "<------------1-------");
                Job job = Job.getInstance(conf, "PageRank");
                job.setJarByClass(PageRank.class);
                job.setMapperClass(PageRank.PRMapper.class);
                // job.setCombinerClass(IntSumReducer.class);
                job.setReducerClass(PageRank.PRReducer.class);
                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(MapWritable.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path("/a2temp/output" + i + "/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("/a2temp/tmpOutput" + i));
                job.waitForCompletion(true); 
                //System.out.println("----------2------------->" + i + "<------------2-------");
                Job jobA = Job.getInstance(conf, "PRAdjust");
                jobA.setJarByClass(PRAdjust.class);
                jobA.setMapperClass(PRAdjust.PRAMapper.class);
                // job.setCombinerClass(IntSumReducer.class);
                jobA.setReducerClass(PRAdjust.PRAReducer.class);
                jobA.setMapOutputKeyClass(LongWritable.class);
                jobA.setMapOutputValueClass(Text.class);
                jobA.setOutputKeyClass(IntWritable.class);
                jobA.setOutputValueClass(Text.class);
                //System.out.println("----------3------------->" + i + "<------------3-------");
                FileInputFormat.addInputPath(jobA, new Path("/a2temp/tmpOutput" + i + "/part-r-00000"));
                FileOutputFormat.setOutputPath(jobA, new Path("/a2temp/output" + (i + 1)));
                //System.out.println("----------4------------->" + i + "<------------4-------");
                jobA.waitForCompletion(true);
                //System.out.println("----------5------------->" + i + "<------------5-------");
        }
        // setup first job for pre-processing input edge data
        Job jobFinal = Job.getInstance(conf, "post-processing");
        jobFinal.setJarByClass(PRPostProcess.class);
        jobFinal.setMapperClass(PRPostProcess.PostMapper.class);
        jobFinal.setReducerClass(PRPostProcess.PostReducer.class);
        jobFinal.setMapOutputKeyClass(IntWritable.class);
        jobFinal.setMapOutputValueClass(DoubleWritable.class);
		jobFinal.setOutputKeyClass(IntWritable.class);
		jobFinal.setOutputValueClass(DoubleWritable.class);
        // jobFinal.setNumReduceTasks(0);

        FileInputFormat.addInputPath(jobFinal, new Path("/a2temp/output" + numIterations + "/part-r-00000"));
        FileOutputFormat.setOutputPath(jobFinal, new Path(args[3]));

        System.exit(jobFinal.waitForCompletion(true) ? 0 : 1);
    }
}