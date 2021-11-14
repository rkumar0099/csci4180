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
    private List<IntWritable> data; //list to store Tuple for a given vertex v
    private IntWritable size;

    public AdjacencyList() {
      this.data = new ArrayList<IntWritable>();
      this.size = new IntWritable(0);
    }

    public AdjacencyList(int cap) {
      this.data = new ArrayList<IntWritable>(cap);
      this.size = new IntWritable(0);
    }
    
    public int getIntSize(){
    	return this.size.get();
    }

    public IntWritable getSize(){
    	return this.size;
    }

    public IntWritable getValue(int index) {
      return this.data.get(index);
    }

    public int getIntValue(int index) {
      return this.data.get(index).get();
    }

    public void add(int val, int dist) {
      this.data.add(new IntWritable(val));
      this.data.add(new IntWritable(dist));
      this.size.set(this.size.get() + 2);
    }

    public void add(IntWritable val, IntWritable dist) {
      this.data.add(val);
      this.data.add(dist);
      this.size.set(this.size.get() + 2);
    }

    public void merge(AdjacencyList list) {
      int listSize = list.getIntSize();
      this.size.set(this.size.get() + listSize);

      for (int i = 0; i < listSize; i++) {
        this.data.add(list.getValue(i));
      }

    }

    @Override
    public void write(DataOutput output) throws IOException {
      this.size.write(output);
      for (int i = 0; i < this.size.get(); i++) {
        this.data.get(i).write(output);
      }
    }

    @Override  
    public void readFields(DataInput input) throws IOException  {
      this.size = new IntWritable(0);
      this.size.readFields(input);

      this.data = new ArrayList<IntWritable>();
      for (int i = 0; i < this.size.get(); i += 2) {
        IntWritable val = new IntWritable();
        IntWritable dist = new IntWritable();
        val.readFields(input);
        dist.readFields(input);
        this.data.add(val);
        this.data.add(dist);
      }
    }

    @Override
    public String toString() {
        String s = "";
        return s;
  }
}