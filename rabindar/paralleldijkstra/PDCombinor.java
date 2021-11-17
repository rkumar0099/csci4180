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


public class PDCombinor extends Reducer<IntWritable, MapOutput, IntWritable, MapOutput> {
    IntWritable unreachable = new IntWritable(-1);
    IntWritable distance = new IntWritable();
    PDNodeWritable node = new PDNodeWritable();
    Tuple e = new Tuple();
		  
	public void reduce(IntWritable nodeId, Iterable<MapOutput> mapOutputs, Context context)
        throws IOException, InterruptedException {
            
            int source = Integer.valueOf(context.getConfiguration().get("source"));
            int minDist = -1;
            int minPath = -1;
            int currDist; //placeholder for distance value
            node = null; 
            Iterator<MapOutput> mapOutput = mapOutputs.iterator(); 

            while(mapOutput.hasNext()) {
                Writable value = mapOutput.next().get();

                    if(value instanceof PDNodeWritable) { //if you receive a node
                        if(node == null) {
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
                        
                }
                
                if(node != null){
                    node.setDistance(minDist);
                    node.setPath(minPath);
                  //  System.out.println("Node:" + node);
                    context.write(nodeId, new MapOutput(node));
                   }
                   else{
                    e.setVertex(new IntWritable(minPath));
                    e.setCost(new IntWritable(minDist));
                    context.write(nodeId, new MapOutput(e));
            }
        }
}
