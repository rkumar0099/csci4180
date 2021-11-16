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

public class PRPostProcess {
    

    public static class PostMapper
            extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
            
            public void map(LongWritable key, Text nodeString, Context context
                    ) throws IOException, InterruptedException {
                PRNodeWritable nodeStructure = new PRNodeWritable();
                nodeStructure.fromString(nodeString.toString());
                context.write(new IntWritable (nodeStructure.getNodeID()),new DoubleWritable (nodeStructure.getPageRank()));
            }
    }

    public static class PostReducer
            extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {            
            
            public void reduce(IntWritable nodeID, Iterable<DoubleWritable> values, Context context
                    ) throws IOException, InterruptedException {
                
                for (DoubleWritable val : values) {
                    context.write(nodeID, val);
                }
            }
    }

    
}

