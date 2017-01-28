package stubs;

import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import org.dataalgorithms.util.HadoopUtil;

public class AggregateByKeyDriver  extends Configured implements Tool{

	
	//private static Logger THE_LOGGER = Logger.getLogger(AggregateByKeyDriver.class);

    public int run(String [] args) throws Exception {
    	Job job = new Job(getConf());
    	
    	job.setJobName("Aggregate By Key Driver");
    	
    	job.setInputFormatClass (TextInputFormat.class);
    	//job.setOutputFormatClass(TextOutputFormat.class);
    	job.setOutputFormatClass(SequenceFileOutputFormat.class);
    	// not suitable for the second job!!!!! 
    	
    	job.setJarByClass(AggregateByKeyDriver.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	
    	job.setMapperClass(AggregateByKeyMapper.class);
    	job.setReducerClass(AggregateByKeyReducer.class);
    	job.setCombinerClass(AggregateByKeyReducer.class);
    	
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	boolean status = job.waitForCompletion(true);
    	//THE_LOGGER.info("run(): status="+status);
    	return status? 0:1;
    }
    
    public static void main (String[] args) throws Exception {
    	if (args.length !=2){
    		//THE_LOGGER.warn("usage AggregateByKeyDriver ");
    		System.exit(1);
    	}
    	
    	//THE_LOGGER.info("inputDir="+args[0]);
    	//THE_LOGGER.info("outputDir= "+args[1]);
    	int returnStatus = ToolRunner.run(new Configuration(), new AggregateByKeyDriver(), args);
    	System.exit(returnStatus);
    }
}
