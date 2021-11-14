import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class PDPreProcess {

    public static class PreMapper
            extends Mapper<Object, Text, IntWritable, MapWritable>{
            
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                while (itr.hasMoreTokens()) {
                    String line = itr.nextToken();
                    
                    if (!line.isEmpty()) {
                        String[] numbers = line.split("\\s+");
                        IntWritable s = new IntWritable();
                        IntWritable t = new IntWritable();
                        IntWritable w = new IntWritable();
                        s.set(Integer.parseInt(numbers[0]));
                        t.set(Integer.parseInt(numbers[1]));
                        w.set(Integer.parseInt(numbers[2]));
                    
                        MapWritable mw = new MapWritable();
                        mw.put(t, w);
                        context.write(s, mw);
                        context.write(t, new MapWritable());
                    }
                }
            }
            
    }

    public static class PreReducer
            extends Reducer<IntWritable,MapWritable,IntWritable,Text> {
            
            public void reduce(IntWritable key, Iterable<MapWritable> values,Context context) 
                throws IOException, InterruptedException {
                PDNodeWritable node;
                MapWritable adjList = new MapWritable();
                Configuration conf = context.getConfiguration();
                int sourceNodeID = Integer.parseInt(conf.get("sourceNodeID"));

                for (MapWritable element : values) {
                    // get adjacency list and add every entry to node's adjList
                    for (Map.Entry entry : element.entrySet()) {
                        adjList.put((IntWritable)entry.getKey(), (IntWritable)entry.getValue());
                    }
                }
                node = new PDNodeWritable(key, adjList);
                if (key.get() == sourceNodeID) {
                    node.setDistance(0);
                }
                context.write(key, new Text(node.toString()));
            }
    }

}
