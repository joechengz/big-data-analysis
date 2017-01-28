import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

//
// remove all invalid fieles; remove all non-word characters
//
public class TextPre {
	
	File dir = new File("/path/to/file");
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
        int hw_num = 1;
		TextPre tp = new TextPre();
		tp.delete();
		tp.filt_out(hw_num);
	}
}
