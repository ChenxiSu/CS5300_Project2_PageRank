import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class GraphReducer extends Reducer<LongWritable, Text, Text, Text>{

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException
	{
		LongWritable curNodeId = key;
		double previousPRValue = 1;
		double nextPRValue = 0;
		double localResidual = 0;
		String edgeListOfCurNode = "";
		long localResidualTransformed=0;

		for(Text value : values){
			String[] inputInfo = value.toString().split("\\s+");

			//incoming pagerank value
			if(inputInfo.length == 1){
				nextPRValue += Double.parseDouble(inputInfo[0]);
			}
			//current node info
			else if(inputInfo.length == 3){
				edgeListOfCurNode = inputInfo[2];
				previousPRValue = Double.parseDouble(inputInfo[1]);
			}
			else if(inputInfo.length == 2){
				previousPRValue = Double.parseDouble(inputInfo[1]);
			}else{
				System.out.println("ERROR: received unexpected TEXT in length");
			}
		}
		if(previousPRValue == 1) System.out.println("No node info has been received by a reducer");
		//calculate the pagerank value according to the given formula
		nextPRValue = pagerankFormula(nextPRValue);

		//should also iterate sink nodes list, add the evenly splitted value

		//reducer should store the updated node info(NPR) to output directory
		context.write(null,new Text(curNodeId+" "+nextPRValue+" "+edgeListOfCurNode));

		//then compare PPR with NPR
		try{
			localResidual = Math.abs(previousPRValue - nextPRValue) / nextPRValue;
			localResidualTransformed = (long)(localResidual*10000);
			//System.out.println("Make sure you got the right transformed residual : "+localResidualTransformed);

		}catch(ArithmeticException e){
			System.out.println("PPR is zero. Check where you get the value!");
		}

		//assume there is a global counter called residualCounter;
		
		context.getCounter(myCounter.ResidualCounter.RESIDUAL_SUM).increment(localResidualTransformed);
		
	}
	//calculate NPR for current Node
	public double pagerankFormula(double sumOfPR){
		double finalPRVal = (1-Utils.DAMPING_FACTOR)/Utils.N + Utils.DAMPING_FACTOR*(sumOfPR);
		return finalPRVal;
	}
}