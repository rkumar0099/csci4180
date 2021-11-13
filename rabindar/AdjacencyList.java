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

public class AdjacencyList implements Writable {
    private List<Edge> data; //list to store Edge for a given vertex v
    private IntWritable size;

    public AdjacencyList() {
      this.data = new ArrayList<Edge>();
      this.size = new IntWritable(0);
    }

    public AdjacencyList(int cap) {
      this.data = new ArrayList<Edge>(cap);
      this.size = new IntWritable(0);
    }
    
    public int getIntSize(){
    	return this.size.get();
    }

    public IntWritable getSize(){
    	return this.size;
    }

    public Edge getEdge(int index) {
      return this.data.get(index)
    }

    public void add(Edge e) {
      this.data.add(e);
      this.size.set(this.size.get() + 1);
    }

    public void merge(AdjacencyList list) {
      int listSize = list.getIntSize();
      this.size.set(this.size.get() + listSize);

      for (int i = 0; i < listSize; i++) {
        this.data.add(list.getEdge(i));
      }

    }

    @Override

    public void write(DataOutput output) {
      this.size.write(output);
      for (Edge e: this.data) {
        e.write(output);
      }
    }

    @Override
    
    public void readFields(DataInput input) {
      this.size = new IntWritable(0);
      this.size.readFields(input);

      this.data = new ArrayList<Edge>();
      for (int i = 0; i < this.size.get(); i++) {
        Edge e = new Edge();
        e.readFields(input);
        this.data.add(e);
      }
    }

    public String toString() {
      String s = "Size: " + Integer.valthis.size.get() + "\n";
      for (Edge e: this.data) {
        s += e.toString();
      }
      return s;
    }

}