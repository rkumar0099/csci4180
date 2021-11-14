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

    public static class MarkSourceMap 
        extends Mapper<IntWritable, AdjacencyList, IntWritable, MapOutput> {
            private PDNodeWritable node = new PDNodeWritable();
            private IntWritable nodeId = new IntWritable();
            private IntWritable unreachable = new IntWritable(-1);

            public void map(IntWritable nodeId, AdjacencyList edges, Context context)
                throws IOException, InterruptedException {
                    int numTuples = edges.getIntSize();
                    int src = Integer.valueOf(context.getConfiguration().get("src"));

                    if (src == nodeId.get()) {
                        context.getCounter(ReachCounter.COUNT).increment(1);
                        node.set(nodeId, 0, true, edges);
                        context.write(nodeId, new MapOutput(node));
                        
                        for (int i = 0; i < numTuples; i++) {
                            Tuple e = edges.get(i);
                            IntWritable v = e.getVertex();
                            e.setVertex(nodeId);
                            context.write(v, new MapOutput(e));
                        }

                    } else {
                        node.set(nodeId, unreachable, false, edges);
                        context.write(nodeId, new MapOutput(node));
                        
                        for (int i = 0; i < numTuples; i++) {
                            Tuple e = edges.get(i);
                            IntWritable v = e.getVertex();
                            e.setVertex(nodeId);
                            context.write(V, new MapOutput(e));
                        }
                    }

                }
            }
    

        public static class Map 
            extends<IntWritable, PDNodeWritable, IntWritable, MapOutput> {
                public void map(IntWritable nodeId, PDNodeWritable node, Context context)
                    throws IOException, InterruptedException {

                private IntWritable nodeId = new IntWritable();
                private IntWritable unreachable = new IntWritable(-1);
                private Tuple e = new Tuple();

                
                AdjacencyList edges = node.getList();
                int numEdges = edges.getIntSize(); //get the size of the adjacency list
                int source = Integer.valueOf(context.getConfiguration().get("src")); //get source node from command line arguments
                int distance = node.getIntDistance(); 
                
                if(distance != -1) {
                  if(!node.isVisited()) {

                    node.markVisited();
                    context.getCounter(ReachCounter.COUNT).increment(1);
                    //dcrease count
                  }
                  else {}             
                  context.write(nodeId, new MapOutput(node));

                  for (int i = 0; i < listSize; i++) {
                    Tuple e = edges.get(i);
                    IntWritable v = e.getVertex();
                    e.setVertex(nodeId);
                    context.write(v, new MapOutput(e));
                  }
                }
                
                else {
                    node.set(nodeId,unreachable, false, edges); //mark visited and distance 0 if the node is source. 
                    context.write(nodeId, new MapOutput(node)); //pass on the graph structure
                    e = new Tuple(nodeId, unreachable);

                    for(int i = 0; i < numEdges; i++) {
                        context.write(edges.get(i).getVertex(), new MapOutput(e));
                }    
            }
        }
    }

    public static class Reduce extends <IntWritable, MapOutput, IntWritable, PDNodeWritable> {
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
                Iterator<MapOutput> mapOutput = mapOutputs.iterator(); 

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
                    if(node != null){
                        node.setDistance(minDist);
                        node.setPath(minPath);
                        context.write(nodeId, node);
                    }
            }
    }

public static void main(String[] args) throws Exception {
		 
	Configuration conf = new Configuration();


	//conf.set("fs.defaultFS", "file:///"); //Remove these comments to run on local mode
	//conf.set("mapreduce.framework.name", "local");
    conf.set("source", args[2]);
    conf.set("mapreduce.output.textoutputformat.separator" , " ");
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(args[0],"temp"), true);
    fs.delete(new Path(args[1]), true);
    
   
    int itr = Integer.valueOf(args[3]);
    long reachCount = -1;
    Job preprocess; 
    Job job; 

    preprocess = new Job(conf, "pdpreprcess");
    preprocess.setMapOutputKeyClass(IntWritable.class);
    preprocess.setMapOutputValueClass(AdjacencyList.class);
    preprocess.setOutputKeyClass(IntWritable.class);
    preprocess.setOutputValueClass(AdjacencyList.class);
    preprocess.setJarByClass(PDPreProcess.class);
    preprocess.setMapperClass(PDPreProcess.Map.class); //dont forget to use other class suffix to distinguish between two mappers
    preprocess.setCombinerClass(PDPreProcess.Reduce.class);
    preprocess.setReducerClass(PDPreProcess.Reduce.class);
    preprocess.setInputFormatClass(TextInputFormat.class);
    preprocess.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(preprocess, new Path(args[0])); //dont forget to delete this file for reruns
    FileOutputFormat.setOutputPath(preprocess, new Path(args[0],"temp/temp0")); 
    preprocess.waitForCompletion(true);
    }
}




    /*
    if(itr == 1)
    {
      job = onlyJob(conf, "paralleldijkstra-solo" , args[0],args[1]);
      job.waitForCompletion(true);
     }
    else if (itr > 1)
    { job = firstJob(conf ,"paralleldijkstra-first", args[0],args[1]);
      job.waitForCompletion(true);
      

      for(int i = 1; i < itr-1; i++)
      {
        job = getJob(conf,"paralleldijkstra"+Integer.toString(i),args[0],args[1], i);
        job.waitForCompletion(true);
        
      }

      job = lastJob(conf,"paralleldijkstra-last",args[0],args[1], itr);
      job.waitForCompletion(true);
      
     
    }else{
      
      job = firstJob(conf ,"paralleldijkstra-first", args[0],args[1]);
      job.waitForCompletion(true);
      reachCount = job.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();

     
      if(reachCount == 0)
      {
      job = lastJob(conf,"paralleldijkstra-last",args[0],args[1], 2);
      job.waitForCompletion(true);
      reachCount = job.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();
    
      }else{ 
        int i = 1;
        while(reachCount != 0)
        {

        job = getJob(conf,"paralleldijkstra"+Integer.toString(i),args[0],args[1], i);
        job.waitForCompletion(true);
        reachCount = job.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();
     
        i++;
        }
          job = lastJob(conf,"paralleldijkstra-last",args[0],args[1], i+1);
          job.waitForCompletion(true);
      }    

      


    }

   

    fs.delete(new Path(args[0],"temp"), true);
   
      
	}
  
   public static Job onlyJob(Configuration conf, String name, String input, String output ) throws Exception{
    Job job = new Job(conf, name);
    

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MapOutput.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntArrayWritable.class);

    job.setJarByClass(ParallelDijkstra.class);

    job.setMapperClass(FirstMap.class);
    job.setCombinerClass(ReducerCombinor.class); //you need standard reducers for this stage. 
    job.setReducerClass(FinalReduce.class);



    job.setInputFormatClass(SequenceFileInputFormat.class); //file fomrat for custom types, the j1 output and j2 input type must be same.
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(input,"temp/temp0"));//naturally read from the result of first job
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job; 
  }

  public static Job firstJob(Configuration conf, String name, String input, String output ) throws Exception{
    Job job = new Job(conf, name);
    

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MapOutput.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PDNodeWritable.class);

    job.setJarByClass(ParallelDijkstra.class);

    job.setMapperClass(FirstMap.class);
    job.setCombinerClass(ReducerCombinor.class); //you need standard reducers for this stage. 
    job.setReducerClass(Reduce.class);



    job.setInputFormatClass(SequenceFileInputFormat.class); //file fomrat for custom types, the j1 output and j2 input type must be same.
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(input, "temp/temp0"));//naturally read from the result of first job
    FileOutputFormat.setOutputPath(job, new Path(input, "temp/temp1"));
    return job; 
  }

  public static Job lastJob(Configuration conf, String name, String input, String output, int itr ) throws Exception{
    Job job = new Job(conf, name);
    

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MapOutput.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntArrayWritable.class);

    job.setJarByClass(ParallelDijkstra.class);

    job.setMapperClass(Map.class);
    job.setCombinerClass(ReducerCombinor.class); //you need standard reducers for this stage. 
    job.setReducerClass(FinalReduce.class);



    job.setInputFormatClass(SequenceFileInputFormat.class); //file fomrat for custom types, the j1 output and j2 input type must be same.
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(input, "temp/temp"+Integer.toString(itr-1)));//naturally read from the result of first job
    FileOutputFormat.setOutputPath(job, new Path(output));

    return job; 
  }

   public static Job getJob(Configuration conf, String name, String input, String output, int itr ) throws Exception {
    Job job = new Job(conf, name);
    

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MapOutput.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PDNodeWritable.class);

    job.setJarByClass(ParallelDijkstra.class);

    job.setMapperClass(Map.class);
    job.setCombinerClass(ReducerCombinor.class); //you need standard reducers for this stage. 
    job.setReducerClass(Reduce.class);



    job.setInputFormatClass(SequenceFileInputFormat.class); //file fomrat for custom types, the j1 output and j2 input type must be same.
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(input, "temp/temp"+Integer.toString(itr)));//naturally read from the result of first job
    FileOutputFormat.setOutputPath(job, new Path(input,"temp/temp"+Integer.toString(itr+1)));
    return job; 
  }




  }

  */