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
import org.apache.hadoop.fs.FileSystem;


public static class MarkSourceMap extends Mapper<IntWritable, AdjacencyList, IntWritable, MapOutput> {
    private PDNodeWritable node = new PDNodeWritable();
    private IntWritable nodeId = new IntWritable();
    private IntWritable unreachable = new IntWritable(-1);

    public void map(IntWritable nodeId, AdjacencyList edges, Context context)
        throws IOException, InterruptedException {
            int numTuples = edges.getIntSize();
            int src = Integer.valueOf(context.getConfiguration().get("src"));

            if (src == nodeId.get()) {
                context.getCounter(ReachCounter.COUNT).increment(1);
                node.set(nodeId, 0, true, edges);
                context.write(nodeId, new MapOutput(node));
                
                for (int i = 0; i < numTuples; i++) {
                    Tuple e = edges.get(i);
                    IntWritable v = e.getVertex();
                    e.setVertex(nodeId);
                    context.write(v, new MapOutput(e));
                }

            } else {
                node.set(nodeId, unreachable, false, edges);
                context.write(nodeId, new MapOutput(node));
                
                for (int i = 0; i < numTuples; i++) {
                    Tuple e = edges.get(i);
                    IntWritable v = e.getVertex();
                    e.setVertex(nodeId);
                    context.write(V, new MapOutput(e));
                }
            }

        }
}