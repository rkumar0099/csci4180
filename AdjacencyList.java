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

public class AdjacencyList implements Writable  
  {	private List<IntWritable> data; //list size is multiple of 2, i%2 = 0 for node id and i%2 for node distance.   
  	private IntWritable size;

    public AdjacencyList()
    {
      //System.out.println("Creating List!");
      this.data = new ArrayList <IntWritable>();
      this.size = new IntWritable(0); 
    }

    public AdjacencyList(int cap)
    {
      //System.out.println("Creating List!");
      this.data = new ArrayList <IntWritable>(cap);
      this.size = new IntWritable(0); 
    }


    public int getIntSize(){
    	return this.size.get();
    }

    public IntWritable getSize(){
    	return this.size;
    }

    public int getIntValue(int index){
        return data.get(index).get();
    }

    public IntWritable getValue(int index){
    	return data.get(index);
    }

    public void addData(int nodeid, int distance )
    {
    	data.add(new IntWritable(nodeid));
    	data.add(new IntWritable(distance));
    	size.set(size.get()+2);
    
    }

    public void addData(IntWritable nodeid, IntWritable distance )
    {
    	data.add(nodeid);
    	data.add(distance);
    	size.set(size.get()+2);
    
    }

    public void merge(AdjacencyList list)
    {   int listSize = list.getIntSize();
    	
    	size.set(size.get()+listSize);
    	
    	for(int i = 0; i < listSize; i++)
    	{
    		data.add(list.getValue(i));
    	}
    }
    
    @Override
     //Must overwrite to as writable is an abstract class.
    public void write(DataOutput out)  throws IOException{
    	

    	 
    	int size = this.size.get();
        this.size.write(out);
        for(int i = 0; i < size; i+=2 )
    	{

    		this.data.get(i).write(out);
    		this.data.get(i+1).write(out);
    	//	System.out.println("Mapper writing:" +this.data.get(i).get() + " " + this.data.get(i+1).get());
    	}

    //	toReturn +=	toReturn += "(" + data.get(size-2) + " " +  data.get(size-1) + ")";
    }

      @Override
    //overriding default readFields method. 
    //It de-serializes the byte stream data
    //Must overwrite to as writable is an abstract class.
    public void readFields(DataInput in) throws IOException {
      	
      	this.data = new ArrayList <IntWritable>();
      	this.size = new IntWritable(0); 
        this.size.readFields(in);

        int size = this.size.get();
        for(int i = 0; i < size; i+=2 )
    	{    
    		IntWritable nodeId = new IntWritable();
    	    IntWritable distance = new IntWritable();
    		nodeId.readFields(in);
    		distance.readFields(in);
    		this.data.add(nodeId);
    		this.data.add(distance);
    	//	System.out.println("Reducer reading:" + nodeId.get() + " " + distance.get());
    	}
    }
    //if your output format is Text or if you use print for this type, the to string method will be invoked. 
    public String toString(){
    	int size = this.size.get();
    	String toReturn = "";
    	for(int i = 0; i < size-2; i+=2 )
    	{

    		toReturn += "(" + data.get(i) + " " +  data.get(i+1) + "), ";

    	}
        if(size >= 2){
            toReturn += "(" + data.get(size-2) + " " +  data.get(size-1) + ")";
        }
    
    	return toReturn; 
    }
}