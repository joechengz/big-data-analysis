package test;
import java.io.*;
import java.util.*;

public class analysis1 {
	
	public static void main(String[] args) throws IOException{
		Scanner s1 = new Scanner(new File("/Users/joe/Documents/text/text/pos-words.txt"));
		Scanner s2 = new Scanner(new File("/Users/joe/Documents/text/text/neg-words.txt"));
		ArrayList<String> pos_list = new ArrayList<String>();
		ArrayList<String> neg_list = new ArrayList<String>();
		while (s1.hasNext()){
		    pos_list.add(s1.next());
		}
		s1.close();
		
		while (s2.hasNext()){
		    neg_list.add(s2.next());
		}
		s2.close();
		
		ArrayList<Integer> count_list = new ArrayList<Integer>();
		for(int i=0;i<neg_list.size();i++){
			count_list.add(0);
		}
		int max1=0;
		int max2=0;
		int max3=0;
		int max4=0;
		int max5=0;
		int max6=0;
		int place = 0;
		String com = "ha";
		File folder = new File("/Users/joe/Documents/text/text/hw_reviews_copy/hw1/");// the path to files that need to clean stop words
		File[] listofiles = folder.listFiles();
		for(int i=0;i<listofiles.length;i++){
			//int count = 0;
			File file = listofiles[i];
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line = null;
			//ArrayList<String> list1 = new ArrayList<String>();
			while( (line = br.readLine())!= null ){
	    	    String[] tokens = line.split("\\s+");
	    	    for(int m=0;m<tokens.length;m++){
	    	    	for(int n=0;n<neg_list.size();n++){
	    	    		if(tokens[m].equals(neg_list.get(n))){
	    	    			count_list.set(n, count_list.get(n)+1);
	    	    			//System.out.println(neg_list.get(n)+":"+n+";"+count_list.get(n));
	    	    		}
	    	    	}
	    	    }
			}
		}
		
		ArrayList<Integer> copy_list = new ArrayList<Integer>(count_list);
		Collections.sort(copy_list,Collections.reverseOrder());
		max1 = copy_list.get(0);
		max2 = copy_list.get(1);
		max3 = copy_list.get(2);
		max4 = copy_list.get(3);
		max5 = copy_list.get(4);
		max6 = copy_list.get(5);
		int index = count_list.indexOf(max1);
		System.out.println(neg_list.get(count_list.indexOf(max1))+" "+max1);
		System.out.println(neg_list.get(count_list.indexOf(max2))+" "+max2);
		System.out.println(neg_list.get(count_list.indexOf(max3))+" "+max3);
		System.out.println(neg_list.get(count_list.indexOf(max4))+" "+max4);
		System.out.println(neg_list.get(count_list.indexOf(max5))+" "+max5);
		System.out.println(neg_list.get(count_list.indexOf(max6))+" "+max6);
//		for(int i=0;i<count_list.size();i++){
//			if(count_list.get(i)==10){
//				System.out.println("10"+pos_list.get(i));
//			}
//			if(count_list.get(i)==14){
//				System.out.println(pos_list.get(i));
//			}
//		}
		

	}

}
