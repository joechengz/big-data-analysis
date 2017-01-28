package stubs;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner mpart;

	@Test
	public void testSentimentPartition() {

		mpart=new SentimentPartitioner();
		mpart.setConf(new Configuration());
		int presult,nresult,neuresult;		
		
		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
        
 		/*
		 * TODO implement
		 */  
		presult=mpart.getPartition(new Text("love"), new IntWritable(1), 3);
		nresult=mpart.getPartition(new Text("deadly"), new IntWritable(1),3);
		neuresult=mpart.getPartition(new Text("zodiac"), new IntWritable(1), 3);
		
		assertEquals(0,presult);
		assertEquals(1,nresult);
		assertEquals(2,neuresult);
	}

}
