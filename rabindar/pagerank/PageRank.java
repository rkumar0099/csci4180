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

public class PageRank {

    public static class Map extends Mapper <LongWritable, Text, LongWritable, MapOutput> {
        private Configuration conf;
        private double numNodes;
        
        @Override
        public void setup (Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.numNodes = Double.parseDouble(conf.get("numNodes"));
        }
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            PRNodeWritable node = new PRNodeWritable();
            DoubleWritable temp = new DoubleWritable();

            String line = value.toString();
            String[] subLines = line.split(",");
            String[] words;

            int numNeighbors = 0;
            double pageRank = 0.0;

            if (subLines.length > 1) {
                words = subLines[0].split(" ");
                pageRank = Double.parseDouble(subLines[1]);
            } else {
                words = subLines[0].split(" ");
                pageRank = 1 / this.numNodes;
            }

            numNeighbors = words.length - 1;

            if (numNeighbors > 0)
                temp.set(pageRank / numNeighbors);
            else
                temp.set(0.0);

            LongWritable[] adjacencyList = new LongWritable[numNeighbors];
            for (int i = 0; i < numNeighbors; i++) {
                adjacencyList[i] = new LongWritable(Long.parseLong(words[i + 1]));
                context.write(adjacencyList[i], new MapOutput(temp));
            }

            node.setNodeId(Long.parseLong(words[0]));
            node.setAdjacencyList(adjacencyList);
            node.setPageRank(pageRank);
            context.write(new LongWritable(node.getNodeId()), new MapOutput(node));
        }
    }
    
    public static class Reduce extends Reducer <LongWritable, MapOutput, LongWritable, PRNodeWritable>{
        public void reduce (LongWritable key, Iterable<MapOutput> values, Context context) throws IOException, InterruptedException {
            PRNodeWritable node = null;
            double temp = 0.0;

            for (MapOutput val : values) {
                Writable instance = val.get();
                if (instance instanceof PRNodeWritable) 
                    node = (PRNodeWritable) instance;
                else if (instance instanceof DoubleWritable) 
                    temp = temp + ((DoubleWritable)instance).get();
            }
            if (node != null) {
                long prevMass = context.getCounter(COUNTER.MASS).getValue();
                double tempMass = prevMass / 1000000000000000.0;
                tempMass += temp;
                long newMass = (long)(tempMass * 1000000000000000.0);
                context.getCounter(COUNTER.MASS).setValue(newMass);
               
                node.setPageRank(temp);
                context.write(key, node);
            }
        }
    }

    public static enum COUNTER {
        MASS
    };

    public static double runPR(String input, String output, Configuration conf) throws Exception {
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        Job job = Job.getInstance(conf, "pr-main");
        
        job.setJarByClass(PageRank.class); 

        job.setMapperClass(PageRank.Map.class);
        job.setReducerClass(PageRank.Reduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MapOutput.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);
        
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

        if (job.waitForCompletion(true) == false) {
            System.out.println("page rank main process failed");
            System.exit(1);
        }

        Counters counters = job.getCounters();
        long mass = (counters.findCounter(COUNTER.MASS).getValue());
        return 1.0 - (mass / 1000000000000000.0);
    }

    public static void main(String[] args) throws Exception {
        int numIter = Integer.parseInt(args[1]);
        int i = 1;
        double mass = 0.0;

        String alpha = args[0];
        String input = args[2];
        String output = args[3];
        String temp = "/PRtemp/";

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("alpha", alpha);
        
        
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(output);
        if(fs.exists(path)){
            fs.delete(path, true);
        }
        path = new Path(temp);
        if(fs.exists(path)){
            fs.delete(path, true);
        }

        long numNodes = PRPreProcess.run(input, temp + "/input-" + String.valueOf(i), conf);
        conf.set("numNodes", String.valueOf(numNodes));

        String PRinput = new String();
        String PRoutput = new String();
        String APRinput = new String();
        String APRoutput = new String();

        while (i <= numIter) {

            PRinput = temp + "/input-" + String.valueOf(i);
            PRoutput = temp + "/tinput-" + String.valueOf(i);
            
            mass = runPR(PRinput, PRoutput, conf);

            conf.unset("mass");
            conf.set("mass", String.valueOf(mass));


            APRinput = temp + "/tinput-" + String.valueOf(i);
            if (i == numIter){
                APRoutput = output;
                PRAdjust.runPRAdjustFinal(APRinput, APRoutput, conf);
            }
            else {
                APRoutput = temp + "/input-" + String.valueOf(i+1);
                PRAdjust.runPRAdjust(APRinput, APRoutput, conf);
            }
            i++;
        }
        Path tempPath = new Path(temp);
        if(fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }
    }
}