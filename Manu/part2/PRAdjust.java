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

public class PRAdjust {
    public static class PRAMapper
            extends Mapper<LongWritable, Text, LongWritable, Text>{
            private final Text isNode = new Text("isNode");
            private final Text node = new Text("node");
            private final Text rank = new Text("rank");
            private final IntWritable one = new IntWritable(1);
            private final IntWritable zero = new IntWritable(0);

            public void map(LongWritable key, Text nodeString, Context context
                    ) throws IOException, InterruptedException {
                context.write(key,nodeString);
            }
    }
    public static class PRAReducer
            extends Reducer<LongWritable,Text,IntWritable,Text> {            
            DoubleWritable m;
            ArrayList<PRNodeWritable> pages;
            public void setup(Context context) throws IOException, InterruptedException {
                m = new DoubleWritable(0);
                pages = new ArrayList<PRNodeWritable>();
            }
            public void reduce(LongWritable nodeID, Iterable<Text> nodeString, Context context
                    ) throws IOException, InterruptedException {
                        for (Text node: nodeString){
                            PRNodeWritable M = new PRNodeWritable();
                            M.fromString(node.toString());
                            pages.add(M);
                            //System.out.println( "<<<<<<<<<<<<<<<<" + pages.size() + ">>>>>>>>>>>>>>>>" );
                            m = new DoubleWritable(m.get() + M.getPageRank());
                        }
            }
            protected void cleanup(Context context) throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                double a = Double.parseDouble(conf.get("alpha"));
                double n = Double.parseDouble(conf.get("nodeNum"));
                for (PRNodeWritable page: pages){
                    double rank =  page.getPageRank();
                    //System.out.println("m= " + m.get() + " a=" + a + " n= " + n);
                    //System.out.println("1 --> " + ((a*1.0/n)));
                    //System.out.println("2 --> " + (((1.0-m.get())/n + rank)));
                    //System.out.println("3 --> " + ((1.0-a)*((1.0-m.get())/n + rank)));
                    //System.out.println("5 --> " + ((a*1.0/n) + (1.0-a) * ((1.0-m.get())/n + rank)));

                    rank = (a*1.0/n) + (1.0-a) * ((1.0-m.get())/n + rank);    
                    page.setPageRank(rank);
                    //System.out.println("Cleanup ------------- " + page.toString());
                    context.write(new IntWritable (page.getNodeID()), new Text(page.toString()));
            }
        }
    }
}
