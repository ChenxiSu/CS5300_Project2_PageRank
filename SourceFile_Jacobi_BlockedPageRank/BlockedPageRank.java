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

public class BlockedPageRank{

	public static void main(String args[]) throws IOException{
		String inputDirectory = null;
		String outputDirectory = null;
		boolean converged = false;
		int numberOfFinishedIteration = 0;

		String input = args[0];
		String output = args[1];

		while(converged != true){

			if(numberOfFinishedIteration==0) {
				inputDirectory = input;
				//System.out.println("Using input file " + inputDirectory);
			}
			else inputDirectory = output + "/"+(numberOfFinishedIteration-1)+".txt";
			outputDirectory = output + "/"+numberOfFinishedIteration+".txt";

			Job job = JobRunner.createJob(numberOfFinishedIteration+1, inputDirectory, outputDirectory);
			//increase global iteration number by 1
			//job.getCounter(myCounter.ResidualCounter.ITERATION_NUM).increment(1);
		
		//output two node PR for each block
			try { 
                job.waitForCompletion(true);
            } catch(Exception e) {
                System.err.println("ERROR IN JOB: " + e);
                return;
            }
		
	        //update iteration rounds
		    numberOfFinishedIteration+=1;

		    //calculate average insdie block iterations for 68 blocks
		    long insdieBlockIterations = job.getCounters().findCounter(myCounter.ResidualCounter.OVERALL_INNER_BLOCK_ITERATION).getValue();
		    double averageBlockIterations = insdieBlockIterations/Utils.BLOCK_NUMBER;
		    
		    //decide whether the whole graph has converged or not!
		    long counterValue = job.getCounters().findCounter(myCounter.ResidualCounter.RESIDUAL_SUM).getValue();
		    
		    System.out.println("Iteration "+numberOfFinishedIteration+" average error "+ counterValue/( (double)10000 * Utils.N) );   
		    System.out.println("Iteration "+numberOfFinishedIteration+" average number of iterations per block is "+averageBlockIterations);
		    //condition to stop the whole iteration
		    if(counterValue/10000 < Utils.N*Utils.EPSILON){
		    	converged = true;
		    	System.out.println("CONVERGED at: "+numberOfFinishedIteration+" iteration");
		    } 
		    	    	
		    

		}
	}
}