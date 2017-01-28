package default1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class RemoveStopWords extends UDF {
	
	Text result = new Text();
	
	ArrayList<String> wordsList = new ArrayList<String>();
	Set<String> stopWordsSet = new HashSet<String>();
	File stopwords = new File("/home/training/training_materials/analyst/project2/text/english.stop");
	
	public Text evaluate(String input) throws IOException {
		
		FileInputStream fstream = new FileInputStream(stopwords);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		//Read File Line By Line
        
		String line;
		while ((line = br.readLine()) != null) {
			stopWordsSet.add(line);
		}
		br.close();
		
		// add non stop words into list
		String[] words = input.split("\\s");
		for(String word : words)
	    {
	        if(!stopWordsSet.contains(word))
	        {
	            wordsList.add(word + " ");
	        }
	    }
		
		result.set(wordsList.toString());
		return result;
	}
}
