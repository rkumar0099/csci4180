import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class PDNodeWritable implements Writable{
    private IntWritable nodeID;
    private IntWritable distance;
    private IntWritable prevNodeID;
    private MapWritable adjList;
    
    // Default constructor to allow (de)serialization
    public PDNodeWritable(IntWritable nodeID, MapWritable adjList) { 
        this.nodeID = new IntWritable(nodeID.get());
        // store max int
        this.distance = new IntWritable(Integer.MAX_VALUE);
        // store invalid node ID
        this.prevNodeID = new IntWritable(0);
        this.adjList = new MapWritable(adjList);
    }

    public PDNodeWritable() {
        this.nodeID = new IntWritable();
        this.distance = new IntWritable();
        this.prevNodeID = new IntWritable();
        this.adjList = new MapWritable();
    }

    public void write(DataOutput out) throws IOException {
        nodeID.write(out);
        distance.write(out);
        prevNodeID.write(out);
        adjList.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        nodeID.readFields(in);
        distance.readFields(in);
        prevNodeID.readFields(in);
        adjList.readFields(in);
    }

    public int getDistance() {
        return this.distance.get();
    }

    public void setDistance(int distance) {
        this.distance.set(distance);
    }

    public int getNodeID() {
        return this.nodeID.get();
    }

    public void setNodeID(int nodeID) {
        this.nodeID.set(nodeID);
    }

    public int getPrevNodeID() {
        return this.prevNodeID.get();
    }

    public void setPrevNodeID(int prevNodeID) {
        this.prevNodeID.set(prevNodeID);
    }

    public MapWritable getAdjList() {
        return this.adjList;
    }

    public void setAdjList(MapWritable adjList) {
        for (Map.Entry entry : adjList.entrySet()) {
            this.adjList.put((IntWritable) entry.getKey(), (IntWritable) entry.getValue());
        }
    }

    @Override
    public String toString() {
        String buffer = "";
        buffer = buffer + this.nodeID.toString() + " " + this.distance.toString() + " " + this.prevNodeID.toString();
        for (Map.Entry entry : this.adjList.entrySet()) {
            buffer = buffer + " " + entry.getKey().toString() + " " + entry.getValue().toString();
        }
        return buffer;
    }

    public void fromString(String text) {

        MapWritable adjList = new MapWritable();

        StringTokenizer itr = new StringTokenizer(text, " ");
        // skip key
        itr.nextToken();
        
        this.setNodeID(Integer.parseInt(itr.nextToken()));
        this.setDistance(Integer.parseInt(itr.nextToken()));
        this.setPrevNodeID(Integer.parseInt(itr.nextToken()));
        
        while (itr.hasMoreTokens()) {
            IntWritable a = new IntWritable();
            IntWritable b = new IntWritable();
            a.set(Integer.parseInt(itr.nextToken()));
            b.set(Integer.parseInt(itr.nextToken()));
        
            adjList.put(a, b);
        }
        this.setAdjList(adjList);
        
    }
    
}