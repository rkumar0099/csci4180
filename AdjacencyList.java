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
    private List<Tuple> data; //list to store Tuple for a given vertex v
    private IntWritable size;

    public AdjacencyList() {
      this.data = new ArrayList<Tuple>();
      this.size = new IntWritable(0);
    }

    public AdjacencyList(int cap) {
      this.data = new ArrayList<Tuple>(cap);
      this.size = new IntWritable(0);
    }
    
    public int getIntSize(){
    	return this.size.get();
    }

    public IntWritable getSize(){
    	return this.size;
    }

    public Tuple getTuple(int index) {
      return this.data.get(index);
    }

    public void add(Tuple e) {
      this.data.add(e);
      this.size.set(this.size.get() + 1);
    }

    public void merge(AdjacencyList list) {
      int listSize = list.getIntSize();
      this.size.set(this.size.get() + listSize);

      for (int i = 0; i < listSize; i++) {
        this.data.add(list.getTuple(i));
      }

    }

    @Override
    public void write(DataOutput output) throws IOException {
      this.size.write(output);
      for (Tuple e: this.data) {
        e.write(output);
      }
    }

    @Override  
    public void readFields(DataInput input) throws IOException  {
      this.size = new IntWritable(0);
      this.size.readFields(input);

      this.data = new ArrayList<Tuple>();
      for (int i = 0; i < this.size.get(); i++) {
        Tuple e = new Tuple();
        e.readFields(input);
        this.data.add(e);
      }
    }

    @Override
    public String toString() {
        String s = "";
        int i = 0;
        for (; i < this.getIntSize() - 1; i++) {
            Tuple e = this.data.get(i);
            s += e.toString() + ", ";
        }
            Tuple e = this.data.get(i);
            s += e.toString();
            return s;
        }
}