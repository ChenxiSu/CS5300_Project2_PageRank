import java.io.*;
import java.util.*;

public class nodefilter {
	static double fromNetID = 0.626;
	static double rejectMin = 0.9 * fromNetID;
	static double rejectLimit = rejectMin +0.01;
	static int size = 685230;
	static String inputPath = "/Users/constance2587/Desktop/edges.txt";
	static String outputPath = "/Users/constance2587/Desktop/inputFile.txt";
	
	public static void main(String [] args) {
		System.out.println("rejectMin: " + rejectMin);
		System.out.println("rejectLimit: " + rejectLimit);
		try {
			//create input file
			File inputFile = new File(inputPath);
			FileReader fileReader = new FileReader(inputFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
                  
            //use hashmap to keep interpreted info of edges.txt
            HashMap<Integer, ArrayList<Integer>> selected = new HashMap<Integer, ArrayList<Integer>>();
            String line = null;
    
            //read edges.txt line by line
            long count = 0;
            while((line = bufferedReader.readLine()) != null) {
            	byte[] b = line.getBytes();
            	
            	//interpreted the info in each line
                int source = Integer.parseInt((new String(b, 0, 6)).trim());
                int destination = Integer.parseInt((new String(b, 7, 6)).trim());
                double x = Double.parseDouble((new String(b, 15, 10)).trim());
                
                //if this line is selected, put its parsed info into hashmap
                if (selectInputLine(x)){
                	count++;
                	if (!selected.containsKey(source)){
                    	ArrayList<Integer> list = new ArrayList<Integer>();
                    	list.add(destination);
                    	selected.put(source, list);
                    }else{
                    	selected.get(source).add(destination);
                    }
                }
            }
            bufferedReader.close();  
            System.out.println("number of selected edges: " + count);
            
            //create output file
            File outputFile=new File(outputPath);
            outputFile.createNewFile();
            FileWriter fileWriter=new FileWriter(outputFile);  
            BufferedWriter bufferedWriter=new BufferedWriter(fileWriter); 
            double initialPR = (double)1 / size;
            
            //output format: sourceID pageRank desID1,desID2,...desIDn,...
            for (int i = 0; i <= size - 1; i++){
            	if (selected.containsKey(i)){
            		//write the sourceID and initialPageRank
            		bufferedWriter.write(i + " " + initialPR + " ");
            		//write destinationIDs
            		ArrayList<Integer> list = selected.get(i);
            		bufferedWriter.write(list.get(0) + "");
            		if (list.size() > 1){
            			for (int j = 1; j < list.size(); j++){
                			bufferedWriter.write("," + list.get(j));
                		}
            		}
            		bufferedWriter.newLine();
            	}else{
            		//if none of this source node's edges is selected, it will still be included, 
            		//but with an empty destiantion list
            		bufferedWriter.write(i + " " + initialPR);
            		bufferedWriter.newLine();
            	}
            }
            bufferedWriter.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file");                
        }
        catch(IOException ex) {
            ex.printStackTrace();
        }
	}
	
	//return false if and only if x falls between rejectMin and rejectLimit
	public static boolean selectInputLine(double x){
		return ((x >= rejectMin) && (x < rejectLimit) ? false : true);
	}
}
