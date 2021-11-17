import java.util.*;
import java.io.*;
import java.math.RoundingMode;
import java.text.DecimalFormat;
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

public class PRAdjust {
    public static class MapStart extends Mapper <LongWritable, Text, LongWritable, PRNodeWritable> {
        private Configuration conf;
        private double alpha;
        private double mass;
        private double numNodes;
        
        @Override
        public void setup (Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.alpha = Double.parseDouble(conf.get("alpha"));
            this.mass = Double.parseDouble(conf.get("mass"));
            this.numNodes = Double.parseDouble(conf.get("numNodes"));
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            PRNodeWritable N = new PRNodeWritable();

            String line = value.toString();
            String[] subLines = line.split(",");
            String[] words = subLines[0].split(" ");;
            int numNeighbors = words.length - 1;
            double pageRank =  Double.parseDouble(subLines[1]);

            LongWritable[] adjacencyList = new LongWritable[numNeighbors];
            for (int i = 0; i < numNeighbors; i++) {
                adjacencyList[i] = new LongWritable(Long.parseLong(words[i + 1]));
            }

            double newPageRank = this.alpha * (1.0 / this.numNodes) + (1 - this.alpha) * ((this.mass / this.numNodes) + pageRank);

            N.setNodeId(Long.parseLong(words[0]));
            N.setAdjacencyList(adjacencyList);
            N.setPageRank(newPageRank);

            context.write(new LongWritable(N.getNodeId()), N);
        }
    }
    
    public static class ReduceStart extends Reducer <LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {
        public void reduce (LongWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            for (PRNodeWritable val : values) {
                context.write(key, val);
            }
        }
    }

    public static class MapFinal extends Mapper <LongWritable, Text, LongWritable, DoubleWritable> {
        private Configuration conf;
        private double alpha;
        private double mass;
        private double numNodes;
        
        @Override
        public void setup (Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.alpha = Double.parseDouble(conf.get("alpha"));
            this.mass = Double.parseDouble(conf.get("mass"));
            this.numNodes = Double.parseDouble(conf.get("numNodes"));
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] subLines = line.split(",");
            String[] words = subLines[0].split(" ");
            long nodeId = Long.parseLong(words[0]);
            double pageRank = Double.parseDouble(subLines[1]);

            double newPageRank = this.alpha * (1.0 / this.numNodes) + (1 - this.alpha) * ((this.mass / this.numNodes) + pageRank);

            context.write(new LongWritable(nodeId), new DoubleWritable(newPageRank));
        }
    }
    
    public static class ReduceFinal extends Reducer <LongWritable, DoubleWritable, LongWritable, Text>{
        private Configuration conf;
        private static final Text V = new Text();
        private static final DecimalFormat df = new DecimalFormat("#.######");


        public void reduce (LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            df.setRoundingMode(RoundingMode.HALF_UP);
            for (DoubleWritable val : values) {
                double pageRank = val.get();
                V.set(df.format(pageRank));
                context.write(key, V);
            
            }
        }
    }

    public static void runPRAdjust(String input, String output, Configuration conf) throws Exception {
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        Job job = Job.getInstance(conf, "pr-adjust-start");
        
        job.setJarByClass(PRAdjust.class); 

        job.setMapperClass(PRAdjust.MapStart.class);
        job.setReducerClass(PRAdjust.ReduceStart.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PRNodeWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);
        
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

        if (job.waitForCompletion(true) == false) {
            System.out.println("Failed starting pre adjust process");
            System.exit(1);
        }
    }

    public static void runPRAdjustFinal(String input, String output, Configuration conf) throws Exception {
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        Job job = Job.getInstance(conf, "pr-adjust-final");
        
        job.setJarByClass(PRAdjust.class); 

        job.setMapperClass(PRAdjust.MapFinal.class);
        job.setReducerClass(PRAdjust.ReduceFinal.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

        if (job.waitForCompletion(true) == false) {
            System.out.println("failed final pre adjust process");
            System.exit(1);
        }
    }
}