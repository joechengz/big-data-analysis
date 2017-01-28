package hw3test;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;

public class TestAvgWordLength {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		mapper mapper = new mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);
		reducer reducer = new reducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		}
	

	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("a test try"));
		mapDriver.withOutput(new Text("a"), new IntWritable(1));
		mapDriver.withOutput(new Text("t"), new IntWritable(4));
		mapDriver.withOutput(new Text("t"), new IntWritable(3));
		mapDriver.runTest();
		System.out.println("success");
		System.out.println("a: 1; t: 4; t: 4");
		}
	
	@Test
	public void testReducer() {
	List values = new ArrayList<IntWritable>();
	values.add(new IntWritable(3));
	values.add(new IntWritable(4));
	reduceDriver.withInput(new Text("t"), values);
	reduceDriver.withOutput(new Text("t"), new DoubleWritable(3.5));
	reduceDriver.runTest();
	System.out.println("success");
	System.out.println("t: 3.5");
	}
	
	@Test
	public void testMapReduce() {
	  
	mapReduceDriver.withInput(new LongWritable(1),
	new Text("bed ark bronx"));
	  
	mapReduceDriver.addOutput(new Text("a"), new DoubleWritable(3.0));
	mapReduceDriver.addOutput(new Text("b"), new DoubleWritable(4.0));
	mapReduceDriver.runTest();
	System.out.println("success");
	System.out.println("a: 3.0; b: 4.0");
	}
	
	public void output(){
		
	}
}

