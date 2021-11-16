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


public class PRPreProcess {
    public static enum num {COUNT};
    public static class PreMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                IntWritable s = new IntWritable();
                IntWritable t = new IntWritable();
                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                //System.out.println("------------Mapper---------------");
                while (itr.hasMoreTokens()) {
                    String line = itr.nextToken();
                    if (!line.isEmpty()) {
                        String v[] = line.split(" ");
                        s=new IntWritable(Integer.parseInt(v[0]));
                        t=new IntWritable(Integer.parseInt(v[1]));
                        //System.out.println("s --> " + s.get() + "t --> " + t.get());
                        context.write(s,t);
                        context.write(t,new IntWritable(-1));
                    }
                }
            }
    }

    public static class PreReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,Text> {
            private PRNodeWritable node;
            private ArrayList<IntWritable> adjList;
            public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) 
                throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                adjList = new ArrayList<IntWritable>();
                for (IntWritable value : values) {
                   // System.out.println("key --> " + key.get() + "value --> " + value.get());
                    if (value.get()!=-1){

                        adjList.add(new IntWritable(value.get()));
                    }
                }
                //System.out.println("key = " + key.get());
               // System.out.println();
                node = new PRNodeWritable(key, adjList);
                //System.out.println("node preprocess-> " + node.toString());
                //System.out.println("----------ebana-------------" + new Text(node.toString()) + "----------v rot-------------");
                context.write(key, new Text(node.toString()));
                context.getCounter(num.COUNT).increment(1);
            }
    }
}
