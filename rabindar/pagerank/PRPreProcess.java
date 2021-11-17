import java.util.*;
import java.lang.*;
import java.io.*;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PRPreProcess {
    public static class Map extends Mapper <LongWritable, Text, LongWritable, LongWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] nodes = line.split(" ");
            LongWritable k = new LongWritable(Long.valueOf(nodes[0]));
            LongWritable v = new LongWritable(Long.valueOf(nodes[1]));
            context.write(k, v);
            context.write(v, new LongWritable(-1));
        }
    }
    
    public static class Reduce extends Reducer <LongWritable, LongWritable, LongWritable, Text>{
        public void reduce (LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            String result = new String();
            for (LongWritable val : values) {
                if (val.get() != -1) {
                    result = result + val + " ";
                }
            }
            context.write(key, new Text(result));
            context.getCounter(COUNTER.NUMBER_OF_NODES).increment(1);
        }
    }

    public static enum COUNTER {
        NUMBER_OF_NODES
    };

    public static long run(String input, String output, Configuration conf) throws Exception {
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        Job job = Job.getInstance(conf, "prpreprocess");
        
        job.setJarByClass(PRPreProcess.class); 

        job.setMapperClass(PRPreProcess.Map.class);
        job.setReducerClass(PRPreProcess.Reduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

        if (job.waitForCompletion(true) == false) {
            System.out.println("Failed to complete pre-process");
            System.exit(1);
        }

        Counters counters = job.getCounters();
        return (long) (counters.findCounter(COUNTER.NUMBER_OF_NODES).getValue());
    }
}