package stubs;

import java.util.HashSet;
import java.io.BufferedReader;
import java.util.Set;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>;
  Set<String> negative = new HashSet<String>;

  /**
   * Set up the positive and negative hash set in the setConf method.
   */
  @Override
  public void setConf(Configuration configuration) {
     /*
     * Add the positive and negative words to the respective sets using the files 
     * positive-words.txt and negative-words.txt.
     */
    /*
     * TODO implement 
     */
    BufferedReader p,n;
    String pword,nword;
    File positivefile = new File("positive-words.txt");
    File negativefile = new File("negative-words.txt");
    try{
      p = new BufferedReader(new FileReader(positivefile));
      while((pword=p.readLine())!=null){
        if(pword.contains(";")){
          continue;
        }
        else{
          positive.add(pword);
        }
      }
      n = new BufferedReader(new FileReader(negativefile));
      while((nword=n.readLine())!=null){
        if(nword.contains(";")){
          continue;
        }
        else{
          negative.add(nword);
        }
      }
    }catch(FileNotFoundException e){
      System.out.println("error");
    }catch(IOException e){
      System.out.println("error");
    }     
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    /*
     * TODO implement
     * Change the return 0 statement below to return the number of the sentiment 
     * category; use 0 for positive words, 1 for negative words, and 2 for neutral words. 
     * Use the sets of positive and negative words to find out the sentiment.
     *
     * Hint: use positive.contains(key.toString()) and negative.contains(key.toString())
     * If a word appears in both lists assume it is positive. That is, once you found 
     * that a word is in the positive list you do not need to check if it is in the 
     * negative list. 
     */
     String word = key.toString();
     if(positive.contains(word)){
      return 0;
     }
     else if(negative.contains(word)){
       return 1;
     }
     else
       return 2;
  }
}
