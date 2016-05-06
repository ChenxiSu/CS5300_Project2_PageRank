import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.ArrayList;


public class BlockedMapper extends Mapper<Text, Text, LongWritable, Text>{
	public void map(Text inputKey, Text inputValue, Context context) throws IOException, InterruptedException{
		
		// inputKey contains the whole line of the txt file
		// inputValue is empty
		String[] inputInfo = inputKey.toString().split("\\s+");
		long curNodeId = Long.parseLong(inputInfo[0]);
		//LongWritable nodeId = new LongWritable(curNodeId);
		double curPR = Double.parseDouble(inputInfo[1]);
		String[] outputNodeIdArray;
		//get the Block ID
		long curBlockId = BlockMatch.blockIDofNode(curNodeId);
		LongWritable blockId = new LongWritable(curBlockId);

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
				long destNodeId = Long.parseLong(nodeStr);
				long destBlockId = BlockMatch.blockIDofNode(destNodeId);
				//LongWritable destNodeId = new LongWritable();

				LongWritable outputKey = new LongWritable(destBlockId);

				//format of output: source dest PR
				//the edge info will go to the block where the destNode locates

				Text outputValue;
				//destNode also in the same block, add 1 in the end
				if(destBlockId == curBlockId){
					outputValue = new Text(curNodeId+" "+destNodeId+" "+curPR/n+" "+1);
				}
				//destNode not in the same block, add 0 in the end
				else{
					outputValue = new Text(curNodeId+" "+destNodeId+" "+curPR/n+" "+0);
				}
				 

				context.write(outputKey, outputValue);//when split, size is only 1
			}
			
		}

		//no matter what kind of node, emit itself to reducer for reconstruction
		// input key has length of 3 after split, now we can differentiate it
		context.write(blockId, inputKey);



	}
}