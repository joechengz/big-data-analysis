package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String line=value.toString();
	  String parts[] = line.split("\\r?\\n");
	  int len = parts.length;
	  for(int i=0;i<len;i++){
		  String[] fields = parts[i].split("\\s+");
		  context.write(new Text(fields[0]), new IntWritable(1));
	  }

  }
}