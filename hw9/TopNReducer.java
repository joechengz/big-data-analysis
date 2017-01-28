package topN;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends
	Reducer<NullWritable, Text, IntWritable, Text>{
	
	private int N=15;
	private SortedMap<Integer, String> top = new TreeMap<Integer, String>();
	
	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
		throws IOException,InterruptedException{
		for (Text value : values){
			String valueAsString = value.toString().trim();
			String[] tokens = valueAsString.split(",");
			String url = tokens[0];
			int frequency = Integer.parseInt(tokens[1]);
			top.put(frequency, url);
			
			if (top.size()>N){
				top.remove(top.firstKey());
			}
		}
		
		File movieTitle = new File("movie_titles.txt");
		if (movieTitle.exists()){
			List<Integer> keys = new ArrayList<Integer>(top.keySet());
			String ID;
			for(int i=keys.size()-1; i>=0; i--){
				//context.write(new IntWritable(keys.get(i)), new Text(top.get(keys.get(i))));
				ID = top.get(keys.get(i)) ;
				int numID = Integer.parseInt(ID);
				InputStreamReader InputStream = new InputStreamReader (new FileInputStream(movieTitle));
				BufferedReader buffReader = new BufferedReader(InputStream);
				
				String line;
				int current=1;
				while((line = buffReader.readLine())!= null && current != numID){
					current++;
				}
				
				InputStream.close();
				buffReader.close();
				String [] linetoken = line.split(",");
				context.write(new IntWritable(keys.get(i)), new Text(linetoken[2]));
				
			}
			
		}
		
		
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
	
		this.N = context.getConfiguration().getInt("N", 15);
	}
}
