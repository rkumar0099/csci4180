import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import java.io.IOException;
import java.util.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.FileSystem;



public class ParallelDijkstra {
    public static enum ReachCounter { COUNT };

    public static class MarkSourceMap extends Mapper<IntWritable, AdjacencyList, IntWritable, MapOutput> {

        private PDNodeWritable node = new PDNodeWritable();
        private IntWritable nodeId = new IntWritable();
        private IntWritable unreachable = new IntWritable(-1);
        Tuple e = new Tuple();

            public void map(IntWritable nodeId, AdjacencyList edges, Context context)
                throws IOException, InterruptedException {
                    int numEdges = edges.getIntSize();
                    int source = Integer.valueOf(context.getConfiguration().get("source"));
                    String choice = context.getConfiguration().get("choice");

                    if (source == nodeId.get()) {
                        context.getCounter(ReachCounter.COUNT).increment(1);
                        node.set(nodeId, 0, true, edges);
                        context.write(nodeId, new MapOutput(node));
                        
                        for (int i = 0; i < numEdges; i += 2) {
                            e.setVertex(nodeId);
                            if (choice.equals("weighted")) {
                                e.setCost(edges.getValue(i+1));
                            } else {
                                e.setIntCost(1);
                            }
                            context.write(edges.getValue(i), new MapOutput(e));
                        }

                    } else {
                        node.set(nodeId, unreachable, false, edges);
                        context.write(nodeId, new MapOutput(node));

                        e.setVertex(nodeId);
                        e.setCost(unreachable);
                        
                        for (int i = 0; i < numEdges; i += 2) {
                            context.write(edges.getValue(i), new MapOutput(e));
                        }
                    }

                }
    }
    
    public static class Map extends Mapper<IntWritable, PDNodeWritable, IntWritable, MapOutput> {
        IntWritable nodeId = new IntWritable();
        IntWritable unreachable = new IntWritable(-1);
        Tuple e = new Tuple();

        public void map(IntWritable nodeId, PDNodeWritable node, Context context)
            throws IOException, InterruptedException {

                AdjacencyList edges = node.getList();
                int numEdges = edges.getIntSize(); //get the size of the adjacency list
                int source = Integer.valueOf(context.getConfiguration().get("source")); //get source node from command line arguments
                int distance = node.getIntDistance(); 
                String choice = context.getConfiguration().get("choice");

                if(distance != -1) {
                  if(!node.isVisited()) {

                    node.markVisited();
                    context.getCounter(ReachCounter.COUNT).increment(1); //decrease the count
                    
                  }
                  else {}             
                  context.write(nodeId, new MapOutput(node));

                  for (int i = 0; i < numEdges; i += 2) {
                    e.setVertex(nodeId);
                    if (choice.equals("weighted")) {
                        e.setIntCost(edges.getIntValue(i+1)+distance);
                    } else {
                        e.setIntCost(edges.getIntValue(i+1) + 1);
                    }
                    context.write(edges.getValue(i), new MapOutput(e));
                  }
            } else {
                    node.set(nodeId,unreachable, false, edges); //mark visited and distance 0 if the node is source. 
                    context.write(nodeId, new MapOutput(node)); //pass on the graph structure
                    e.setVertex(nodeId);
                    e.setCost(unreachable);
                    for(int i = 0; i < numEdges; i += 2) {
                        context.write(edges.getValue(i), new MapOutput(e));
                    }    
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, MapOutput, IntWritable, PDNodeWritable> {  
        IntWritable unreachable = new IntWritable(-1);
        IntWritable distance = new IntWritable();
        PDNodeWritable node = new PDNodeWritable();
        Tuple path = new Tuple();

        public void reduce(IntWritable nodeId, Iterable<MapOutput> values, Context context)
            throws IOException, InterruptedException {
                int minDist = -1;
                int minPath = -1;
                int currDist; //placeholder for distance value
                node = null; 
                Iterator<MapOutput> mapOutput = values.iterator(); 

                while(mapOutput.hasNext()) {
                    Writable value = mapOutput.next().get();

                    if(value instanceof PDNodeWritable) { //if you receive a node
                        if(node == null){
                            node = new PDNodeWritable();
                            node.set((PDNodeWritable)value);
                        }

                        currDist = ((PDNodeWritable)value).getIntDistance();

                        if(minDist == -1) {
                            minDist = currDist;
                            minPath = ((PDNodeWritable)value).getIntPath();
                            } else if(currDist != -1 && currDist < minDist){
                              minDist = currDist;
                              minPath = ((PDNodeWritable)value).getIntPath();
                            }
                        }

                        else { //received edge distance. 
                            currDist = ((Tuple)value).getIntCost();
                            if(minDist == -1) {
                                minDist = currDist;
                                minPath = ((Tuple)value).getIntVertex();
                            } else{
                                if(currDist != -1 && currDist < minDist){
                                  minDist = currDist;
                                  minPath = ((Tuple)value).getIntVertex();
                            }
              
                        }
              
                    }
                        
                } //check all mapper result and write.
                    if(node != null) {
                        node.setDistance(minDist);
                        node.setPath(minPath);
                        context.write(nodeId, node);
                    }
            }
    }

public static void main(String[] args) throws Exception {
		 
	Configuration conf = new Configuration();
    long startTime = System.currentTimeMillis();

	//conf.set("fs.defaultFS", "file:///"); //Remove these comments to run on local mode
	//conf.set("mapreduce.framework.name", "local");
    conf.set("source", args[2]);
    conf.set("choice", args[4]);
    conf.set("mapreduce.output.textoutputformat.separator" , " ");
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(args[0],"temp"), true);
    fs.delete(new Path(args[1]), true);
    
   
    int itr = Integer.valueOf(args[3]);
    long reachCount = -1;
    Job pdpreprocess; 
    Job job; 

    pdpreprocess = new Job(conf, "pdpreprcess");
    pdpreprocess.setMapOutputKeyClass(IntWritable.class);
    pdpreprocess.setMapOutputValueClass(AdjacencyList.class);
    pdpreprocess.setOutputKeyClass(IntWritable.class);
    pdpreprocess.setOutputValueClass(AdjacencyList.class);
    pdpreprocess.setJarByClass(PDPreProcess.class);
    pdpreprocess.setMapperClass(PDPreProcess.Map.class); //dont forget to use other class suffix to distinguish between two mappers
    pdpreprocess.setCombinerClass(PDPreProcess.Reduce.class);
    pdpreprocess.setReducerClass(PDPreProcess.Reduce.class);
    pdpreprocess.setInputFormatClass(TextInputFormat.class);
    pdpreprocess.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(pdpreprocess, new Path(args[0])); //dont forget to delete this file for reruns
    FileOutputFormat.setOutputPath(pdpreprocess, new Path(args[0],"temp/temp0")); 
    pdpreprocess.waitForCompletion(true);


    if(itr == 1)
    {
      job = itrOneJob(conf, "pd-only" , args[0],args[1]);
      job.waitForCompletion(true);
      reachCount = job.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();
    }
     
    else if (itr > 1) {
      job = initialJob(conf ,"pd-initial", args[0],args[1]);
      job.waitForCompletion(true);
      

      for(int i = 1; i < itr-1; i++)
      {
        job = midJob(conf,"pd"+Integer.toString(i),args[0],args[1], i);
        job.waitForCompletion(true);
        
      }

      job = finalJob(conf,"pd-final",args[0],args[1], itr);
      job.waitForCompletion(true);
      
     
    }else{
      
      job = initialJob(conf ,"pd-initial", args[0],args[1]);
      job.waitForCompletion(true);
      reachCount = job.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();

     
      if(reachCount == 0)
      {
      job = finalJob(conf,"pd-final",args[0],args[1], 2);
      job.waitForCompletion(true);
      reachCount = job.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();
    
      }else{ 
        int i = 1;
        while(reachCount != 0)
        {

        job = midJob(conf,"pd"+Integer.toString(i),args[0],args[1], i);
        job.waitForCompletion(true);
        reachCount = job.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();
     
        i++;
        }
          job = finalJob(conf,"pd-final",args[0],args[1], i+1);
          job.waitForCompletion(true);
      }
    }


    fs.delete(new Path(args[0],"temp"), true);

    System.out.println("The reach count is " + reachCount);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    
}

    public static Job itrOneJob(Configuration conf, String name, String input, String output ) throws Exception {
        Job job = new Job(conf, name);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MapOutput.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Tuple.class);
    
        job.setJarByClass(ParallelDijkstra.class);
    
        job.setMapperClass(MarkSourceMap.class);
        job.setCombinerClass(PDCombinor.class); //you need standard reducers for this stage. 
        job.setReducerClass(LastReduce.class);
    
    
    
        job.setInputFormatClass(SequenceFileInputFormat.class); //file fomrat for custom types, the j1 output and j2 input type must be same.
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input,"temp/temp0"));//naturally read from the result of first job
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job; 
    }

  
    public static Job initialJob(Configuration conf, String name, String input, String output ) throws Exception{
            Job job = new Job(conf, name);
            
        
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(MapOutput.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(PDNodeWritable.class);
        
            job.setJarByClass(ParallelDijkstra.class);
        
            job.setMapperClass(MarkSourceMap.class);
            job.setCombinerClass(PDCombinor.class); //you need standard reducers for this stage. 
            job.setReducerClass(Reduce.class);
        
        
        
            job.setInputFormatClass(SequenceFileInputFormat.class); //file fomrat for custom types, the j1 output and j2 input type must be same.
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(input, "temp/temp0"));//naturally read from the result of first job
            FileOutputFormat.setOutputPath(job, new Path(input, "temp/temp1"));
            return job; 
          }
        
          public static Job finalJob(Configuration conf, String name, String input, String output, int itr ) throws Exception{
            Job job = new Job(conf, name);
            
        
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(MapOutput.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Tuple.class);
        
            job.setJarByClass(ParallelDijkstra.class);
        
            job.setMapperClass(Map.class);
            job.setCombinerClass(PDCombinor.class); //you need standard reducers for this stage. 
            job.setReducerClass(LastReduce.class);
        
        
        
            job.setInputFormatClass(SequenceFileInputFormat.class); //file fomrat for custom types, the j1 output and j2 input type must be same.
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(input, "temp/temp"+Integer.toString(itr-1)));//naturally read from the result of first job
            FileOutputFormat.setOutputPath(job, new Path(output));
        
            return job; 
          }
        
           public static Job midJob(Configuration conf, String name, String input, String output, int itr ) throws Exception {
            Job job = new Job(conf, name);
            
        
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(MapOutput.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(PDNodeWritable.class);
        
            job.setJarByClass(ParallelDijkstra.class);
        
            job.setMapperClass(Map.class);
            job.setCombinerClass(PDCombinor.class); //you need standard reducers for this stage. 
            job.setReducerClass(Reduce.class);
        
        
        
            job.setInputFormatClass(SequenceFileInputFormat.class); //file fomrat for custom types, the j1 output and j2 input type must be same.
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(input, "temp/temp"+Integer.toString(itr)));//naturally read from the result of first job
            FileOutputFormat.setOutputPath(job, new Path(input,"temp/temp"+Integer.toString(itr+1)));
            return job; 
          }
        
        
        
        
        }