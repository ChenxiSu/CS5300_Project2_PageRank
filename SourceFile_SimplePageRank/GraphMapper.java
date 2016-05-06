import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.ArrayList;

public class GraphMapper extends Mapper<Text, Text, LongWritable, Text>{
	
	public void map(Text inputKey, Text inputValue, Context context) throws IOException, InterruptedException{

		// inputKey contains the whole line of the txt file
		// inputValue is empty
		String[] inputInfo = inputKey.toString().split("\\s+");
		LongWritable nodeId = new LongWritable(Long.parseLong(inputInfo[0]));
		double curPR = Double.parseDouble(inputInfo[1]);
		String[] outputNodeIdArray;

		//sink node
		if(inputInfo.length == 2) {
			//update a global sinkNode map, with ID and PR,
			//so when adding PR together in pagerank, a node can fetch PR from
			//the sink node automatically
		}
		//normal node 
		else{
			outputNodeIdArray = inputInfo[2].split(",");
			int n =outputNodeIdArray.length;

			for(String nodeStr : outputNodeIdArray){
				LongWritable outputKey = new LongWritable( Long.parseLong(nodeStr) );
				Text outputValue = new Text(""+curPR/n);
				context.write( outputKey, outputValue);//when split, size is only 1
			}
			
		}
		
		//no matter what kind of node, emit itself to reducer for reconstruction
		context.write(nodeId, inputKey);
	}
}
