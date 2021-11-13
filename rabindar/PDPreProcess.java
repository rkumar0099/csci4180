import java.io.IOException;
import java.util.*;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PDPreProcess {

    public static class Map extends 
			Mapper<LongWritable, Text, IntWritable, AdjacencyList> {
                private IntWritable nodeId = new IntWritable();
                private AdjacencyList list = new AdjacencyList(1);
                private BooleanWritable self = new BooleanWritable(false);

                public void map(LongWritable key, Text text, Context context)
                    throws IOException, InterruptedException {
                        int src = Integer.valueOf(context.getConfiguration.get("src"));
                        String line = text.toString();
                        StringTokenizer tokens = new StringTokenizer(line);
                        node.set(Integer.valueOf(tokens.nextToken()));
                        Edge e = new Edge(Integer.valueOf(tokens.nextToken, tokens.nextToken));
                        list.add(e);
                        context.write(nodeId, list);
                        
                    }
            }

    public static class Reduce 
        extends Reducer<IntWritable, AdjacencyList, IntWritable, AdjacencyList> {
            AdjacencyList endList;

            public void reduce(IntWritable key, Iterable<AdjacencyList> lists, Context context) 
                throws IOException, InterruptedException {
                    for (AdjacencyList list: lists) {
                        endList.merge(list);
                    }
                    context.write(key, endList);
                }
        }


        public static void main(String[] args) throws Exception {
            // Run on a local node 
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "file:///");
            conf.set("mapreduce.framework.name", "local");
    
    
    
            Job job = Job.getInstance(conf, "pdpreprocess");
    
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(AdjacencyList.class);
    
            job.setJarByClass(PDPreProcess.class);
    
            job.setMapperClass(Map.class);
            //job.setCombinerClass(Reduce.class);
            job.setReducerClass(Reduce.class);
    
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
            job.waitForCompletion(true);
        }

        
    }