import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class SimplePageRank{
	public static void main(String args[]) throws IOException{

		System.out.println("SimplePageRank starts running.");
		int mrPassNumber = 5;
		String input = args[0];
		String output = args[1];
		String inputDirectory = null;
		String outputDirectory = null;
		
		//job iteration
		for(int i=0; i<mrPassNumber; i++){
			//update input and output directory
			if(i==0) {
				//File inputFile = new File("input/inputFile.txt");
				//inputDirectory = inputFile.getPath();
				inputDirectory = input;
				//System.out.println("Using input file " + inputDirectory);
			}
			else inputDirectory = output+"/"+(i-1)+".txt";
			outputDirectory = output+"/"+i+".txt";


			Job job = JobRunner.createJob(i,inputDirectory,outputDirectory);
			
			try { 
                job.waitForCompletion(true);
            } catch(Exception e) {
                System.err.println("ERROR IN JOB: " + e);
                return;
            }

			//decide whether to stop or continue
			long counterValue = job.getCounters().findCounter(myCounter.ResidualCounter.RESIDUAL_SUM).getValue();
			if(counterValue < Utils.N * 10000 * Utils.EPSILON){
				System.out.println("PageRank has converged!");
				System.out.println("Iteration number is: "+(i+1));
				break;
			}else{
				System.out.println("Iteration "+(i+1)+" average error "+((double)counterValue)/(10000*Utils.N));
			}
		}
	}
}