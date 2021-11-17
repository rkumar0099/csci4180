import org.apache.hadoop.io.*;
import java.io.*;

public class PRNodeWritable implements Writable {

    public static class LongArrayWritable extends ArrayWritable {
        public LongArrayWritable() {
            super(LongWritable.class);
        }
    }

    private LongWritable nodeId;
    private LongArrayWritable adjacencyList;
    private DoubleWritable pageRank;

    public PRNodeWritable() {
        this.nodeId = new LongWritable();
        this.adjacencyList = new LongArrayWritable();
        this.pageRank = new DoubleWritable();
    }

    public void setNodeId(long Id) {
        this.nodeId.set(Id);
    }
    public long getNodeId() {
        return this.nodeId.get();
    }

    public void setAdjacencyList(LongWritable[] values) {
        this.adjacencyList.set(values);
    }

    public LongWritable[] getAdjacencyList() {
        Writable[] values = this.adjacencyList.get();
        LongWritable[] result = new LongWritable[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (LongWritable) values[i];
        }
        return result;
    }

    public void setPageRank(double val) {
        this.pageRank.set(val);
    }
    public double getPageRank() {
        return this.pageRank.get();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.nodeId.readFields(input);
        this.adjacencyList.readFields(input);
        this.pageRank.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        this.nodeId.write(output);
        this.adjacencyList.write(output);
        this.pageRank.write(output);
    }

    @Override
    public String toString() {
        String result = new String();
        for (LongWritable node : this.getAdjacencyList()) {
            result = result + node.toString() + " ";              
        }
        result = result + "," + this.pageRank;
        return result;
    }
}
