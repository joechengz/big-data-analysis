import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class TextPre {
	
	File dir = new File("/Users/leoyang/Documents/tt/");
	Scanner sc;
	public void delete() throws FileNotFoundException {
		
		for(File file: dir.listFiles()){
			FileInputStream fis = new FileInputStream(file);
			int count=0;
			sc = new Scanner(fis);
			while(sc.hasNext()) {
				sc.next();
				count++;
			}
			if(count < 50) {
				file.delete();
			}
		}	
	}
	
	public void filt_out(int id) throws IOException {
		
		BufferedReader bin;
		
		for(File file: dir.listFiles()) {
			
			FileInputStream fis = new FileInputStream(file);
			bin = new BufferedReader(new InputStreamReader(fis));
			
			String line = null;
			StringBuffer sb = new StringBuffer();
			
			sb.append(id + "\t");
			
			while((line = bin.readLine()) != null) {
//				line = line.replace("\n", " ").replace("\t", "");
				line = line.replaceAll("\\W", " ");
				sb.append(line);
			}
			
			sb.append("\n");
			
			try{
				FileWriter fw = new FileWriter(file);
				fw.write(sb.toString().toLowerCase());
				fw.close();
			}
			catch(IOException e) {
				e.printStackTrace();
			}			
		}
	}
	
	public static void main(String args[]) throws IOException {
		TextPre tp = new TextPre();
		tp.delete();
		tp.filt_out(4);
	}
}
