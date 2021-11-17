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


public class Tuple implements Writable {
    private IntWritable vertex;
    private IntWritable cost;
    private BooleanWritable fill = new BooleanWritable(false);

    public Tuple() {}

    public Tuple(int v, int c) {
        this.vertex = new IntWritable(v);
        this.cost = new IntWritable(c);
        this.fill.set(true);
    }

    public Tuple(IntWritable v, IntWritable c) {
        this.vertex = v;
        this.cost = c;
       this.fill.set(true);
    }

    public IntWritable getVertex() {
        return this.vertex;
    }

    public IntWritable getCost() {
        return this.cost;
    }

    public int getIntVertex() {
        return this.vertex.get();
    }

    public int getIntCost() {
        return this.cost.get();
    }

    public void setVertex(IntWritable v) {
        this.vertex = v;
        this.fill.set(true);
    }

    public void setIntVertex(int v) {
        this.vertex = new IntWritable(v);
    }

    public void setCost(IntWritable c) {
        this.cost = c;
    }

    public void setIntCost(int c) {
        this.cost = new IntWritable(c);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        this.vertex.write(output);
        this.cost.write(output);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.vertex = new IntWritable(0);
        this.cost = new IntWritable(0);

        this.vertex.readFields(input);
        this.cost.readFields(input);
    }

    @Override
    public String toString() {
        if (!this.fill.get()) {
            return "";
        }
        if (this.getIntVertex() == -1) {
            return this.getIntCost() + " nil";
        }
        return  this.getIntCost() + " " + this.getIntVertex();
    }

}