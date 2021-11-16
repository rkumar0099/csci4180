import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;

public class PRNodeWritable implements Writable{
    private IntWritable nodeID;
    private DoubleWritable pageRank;
    private ArrayList<IntWritable> adjList;
    
    // Default constructor to allow (de)serialization
    public PRNodeWritable(IntWritable nodeID, ArrayList<IntWritable> adjList) { 
        this.nodeID = new IntWritable(nodeID.get());
        this.pageRank = new DoubleWritable(-1);
        // store invalid node ID
        this.adjList = new ArrayList<>(adjList);
    }

    public PRNodeWritable() {
        this.nodeID = new IntWritable();
        this.pageRank = new DoubleWritable();
        this.adjList = new ArrayList<IntWritable>();
    }

   public void write(DataOutput out) throws IOException {
        this.nodeID.write(out);
        this.pageRank.write(out);
        IntWritable size = new IntWritable(adjList.size());
        size.write(out);
        for (IntWritable i:adjList){
            i.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        this.nodeID.readFields(in);
        this.pageRank.readFields(in);
        IntWritable numValues = new IntWritable();
        numValues.readFields(in);
        for (int i=0;i<numValues.get();i++){
            IntWritable x = new IntWritable();
            x.readFields(in);
            this.adjList.add(x);
        }
    }

    public double getPageRank() {
        return this.pageRank.get();
    }

    public void setPageRank(double pageRank) {
        this.pageRank.set(pageRank);
    }

    public int getNodeID() {
        return this.nodeID.get();
    }

    public void setNodeID(int nodeID) {
        this.nodeID.set(nodeID);
    }


    public ArrayList<IntWritable> getAdjList() {
        return this.adjList;
    }

    public void setAdjList(ArrayList<IntWritable> adjList) {
        this.adjList = new ArrayList<IntWritable>(adjList);
    }

    @Override
    public String toString() {
        String buffer = "";
        buffer = buffer + this.nodeID.toString() + " " + this.pageRank.toString();
        for (IntWritable entry : this.adjList) {
            buffer = buffer + " " + entry.toString();
        }
        return buffer;
    }

    public void fromString(String text) {
        ////System.out.println("----------------" + text + "----------------");
        ArrayList<IntWritable> adjList = new ArrayList<IntWritable>();
        StringTokenizer itr = new StringTokenizer(text, " ");
        itr.nextToken();
        if (!itr.hasMoreTokens())
        {
            return;
        }
        this.setNodeID(Integer.parseInt(itr.nextToken()));
        this.setPageRank(Double.parseDouble(itr.nextToken()));
        while (itr.hasMoreTokens()) {
            IntWritable a = new IntWritable();
            a.set(Integer.parseInt(itr.nextToken()));
            adjList.add(a);
        }
        this.setAdjList(adjList);
    }
    
}