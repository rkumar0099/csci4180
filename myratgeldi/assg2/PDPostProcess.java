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

public class PDPostProcess {
    

    public static class PostMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{
            
            public void map(LongWritable key, Text nodeString, Context context
                    ) throws IOException, InterruptedException {
                PDNodeWritable nodeStructure = new PDNodeWritable();
                IntWritable nodeID = new IntWritable();
                IntWritable distance = new IntWritable();
                IntWritable prevNodeID = new IntWritable();
                String buffer = "";

                nodeStructure.fromString(nodeString.toString());
                nodeID.set(nodeStructure.getNodeID());
                distance.set(nodeStructure.getDistance());
                prevNodeID.set(nodeStructure.getPrevNodeID());

                if (distance.get() != Integer.MAX_VALUE) {
                    buffer = buffer + distance.get() + " ";
                    if (prevNodeID.get() == 0) {
                        buffer = buffer + "nil";
                    }
                    else {
                        buffer = buffer + prevNodeID.get();
                    }
                    context.write(nodeID, new Text(buffer));
                }

            }
    }

    public static class PostReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {            
            
            public void reduce(IntWritable nodeID, Iterable<Text> values, Context context
                    ) throws IOException, InterruptedException {
                
                for (Text val : values) {
                    context.write(nodeID, val);
                }
            }
    }

    
}
