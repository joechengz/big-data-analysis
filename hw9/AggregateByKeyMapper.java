package stubs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class AggregateByKeyMapper extends
       Mapper<LongWritable, Text, Text, IntWritable>{
	
    //private Text k2 = new Text();
    //private IntWritable v2 = new IntWritable(1);
	
	@Override
	public void  map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		
		String [] hi = value.toString().split("\\W+");
		
		String movie= hi[0];
		//k2.set(movie);
		int rat = Integer.valueOf(hi[2]);
		context.write(new Text(movie), new IntWritable(rat));
		
	} 
}

