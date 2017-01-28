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

public class RemoveStopWords {
	ArrayList<String> wordsList = new ArrayList<String>();
	Set<String> stopWordsSet = new HashSet<String>();
	File stopwords = new File("/project2/english.stp");
	
	public String remove(String input) throws IOException {
		
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
		for(String str : wordsList) {
			System.out.print(str);
		}
		
		String pureReview = wordsList.toString();
		return pureReview;
	}
	
}
