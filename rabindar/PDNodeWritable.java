import java.io.IOException;
import java.util.*;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PDNodeWritable implements Writable {
    IntWritable nodeId;
    IntWritable prevId;
    IntWritable distance;
    BooleanWritable visited;
    AdjacencyList edges;

    public PDNodeWritable() {
     this.nodeId = new IntWritable();
     this.prevId = new IntWritable(-1);
     this.distance = new IntWritable();
     this.visited = new BooleanWritable(false);
     this.edges = new AdjacencyList();
    }

    public void set(IntWritable nodeId, IntWritable distance, boolean visited, AdjacencyList edges) {
     this.nodeId = nodeId;
     this.distance = distance;
     this.prevId = new IntWritable(-1);
     this.visited = new BooleanWritable(visited);
     this.edges = edges;
    }

    public void set(IntWritable nodeId, int distance, boolean visited,AdjacencyList edges) {
     this.nodeId = nodeId;
     this.prevId = new IntWritable(-1);
     this.distance = new IntWritable(distance);
     this.visited = new BooleanWritable(visited);
     this.AdjacencyList = edges;
    }

     public void set(PDNodeWritable node) {
    
     this.nodeId = node.nodeId;
     this.prevId = node.prevId;
     this.distance = node.distance;
     this.visited = node.visited;
     this.edges = node.edges;
    }
   
    public AdjacencyList getList() {
        return this.edges;
    }
    
    public void setList(AdjacencyList edges) {
        this.edges = edges;
    }
    

    public void setPath(int path) {
        this.prevId = new IntWritable(path);
    }

    public void setDistance(int distance) {
        this.distance = new IntWritable(distance);
    }

    public int getIntId() {
        return this.nodeId.get();
    }

    public IntWritable getId() {
        return this.nodeId;
    }

    public int getIntPath() {
        return this.prevId.get();
    }

    public IntWritable getPath() {
        return this.prevId;
    }

    public int getIntDistance() {
        return this.distance.get();
    }

    public IntWritable getDistance() {
        return this.distance;
    }

    public boolean isVisited() {
        return this.visited.get();
    }

    public void markVisited() {
        this.visited = new BooleanWritable(true);
    }

    @Override
    public void write(DataOutput out)  throws IOException {
    	this.nodeId.write(out);
        this.prevId.write(out);
        this.distance.write(out);
        this.visited.write(out);
        this.edges.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      	this.nodeId.readFields(in);
        this.prevId.readFields(in);
        this.distance.readFields(in);
        this.visited.readFields(in);
        this.edges.readFields(in);
    }

    public String toString() {
        if(this.visited.get()){

            return nodeId.toString() + " "  + distance.toString() + " " + prevId.toString() + " visited " + edges.toString(); 
        }
            else{
                return nodeId.toString() + " " + distance.toString() + " " + prevId.toString() + " not visited " + edges.toString(); 
            }
    	
    }
}