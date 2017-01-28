package stubs;

//import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

public class TestProcessLogs {
	MapDriver<LongWritable, Text,Text,IntWritable> mapDriver;
	ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text,Text,IntWritable,Text,IntWritable> mapreduceDriver;
	
	@Before
	public void setUp(){
		LogFileMapper mapper = new LogFileMapper();
		mapDriver = new MapDriver<LongWritable,Text,Text,IntWritable>();
		mapDriver.setMapper(mapper);
		
		SumReducer reducer = new SumReducer();
		reduceDriver = new ReduceDriver<Text,IntWritable,Text,IntWritable>();
		reduceDriver.setReducer(reducer);
		
		mapreduceDriver = new MapReduceDriver<LongWritable, Text,Text,IntWritable,Text,IntWritable>();
		mapreduceDriver.setMapper(mapper);
		mapreduceDriver.setReducer(reducer);
	}
	

	@Test
	public void testMapper() {
		String test = new String("10.11.12.13  - -  shjkdnksjd\n" );
		mapDriver.withInput(new LongWritable(), new Text("10.11.12.13  - - shjkdnksjdfk \n" +
				"10.11.12.13  --sdhjkhjkds \n" +
				"10.13.24.51  - - sdhjfksd"));
	    //mapDriver.withInput(new LongWritable(), new Text("10.13.24.51  - -  shjkdnksjdfk"));
		mapDriver.withOutput(new Text("10.11.12.13"), new IntWritable(1));
		mapDriver.withOutput(new Text("10.11.12.13"), new IntWritable(1));
	    mapDriver.withOutput(new Text("10.13.24.51"), new IntWritable(1));
		mapDriver.runTest();
		
	}
	
	@Test
	public void testReducer(){
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("10.11.12.13"), values);
		reduceDriver.withOutput(new Text("10.11.12.13"), new IntWritable(2));
		reduceDriver.runTest();
		
		
	}
	
	@Test
	public void testdriver(){
		mapreduceDriver.withInput(new LongWritable(),new Text("10.11.12.13  - - shjkdnksjdfk \n" +
				"10.11.12.13  --sdhjkhjkds \n" +
				"10.13.24.51  - - sdhjfksd"));
		List<LongWritable> values = new ArrayList<LongWritable>();
		values.add(new LongWritable(1));
		//values.add(new LongWritable(1));
		//values.add(new LongWritable(1));
		mapreduceDriver.withOutput(new Text("10.11.12.13"), new IntWritable(2));
		mapreduceDriver.withOutput(new Text("10.13.24.51"), new IntWritable(1));
		mapreduceDriver.runTest();
		
	}

}
