import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class BlockedReducer extends Reducer<LongWritable, Text, Text, Text>{

	public void reduce(LongWritable inputKey, Iterable<Text> inputValues, Context context) 
	throws IOException, InterruptedException{

		HashMap<Long, Double> previousPR = new HashMap<Long, Double>();
		HashMap<Long, Double> nextPR = new HashMap<Long, Double>();

		//store the inside coming edges(from the perspective of destNode)
		HashMap<Long, List<Double> > insideNodeComingEdgeList = new HashMap<Long, List<Double>>();
		//store the boundary edges: key-destNode, value-incoming PR
		HashMap<Long, List<Double> > boundaryCondition = new HashMap<Long, List<Double>>();
		HashMap<Long, String> graph = new HashMap<Long, String>();

		long curBlockId = inputKey.get();
		long nodeNumberInBlock = 0;
		//control inner iterations
		long innerIteration = 0;
		boolean insdieBlockIterationConverged = false;

		double totalResidual = 0;
		double terminationCondition = 0.001;

		//iterate all the input and store them into different data structure for inside iteration
		for(Text value : inputValues){

			String[] inputInfo = value.toString().split("\\s+");

			//when receive a node
			if(inputInfo.length <= 3){
				//store node information
				storeIncomingNodeIntoGraphAndUpdatePPR(previousPR, graph, inputInfo);
			}
			//when receive an edge: format- sourceNode destNode PR 1/0
			else{
				int insideOrOutside = Integer.parseInt(inputInfo[3]);
				long destNode = Long.parseLong(inputInfo[1]);
				double curdestNodePR = Double.parseDouble(inputInfo[2]);
				//edge comes from node in the same block
				if(insideOrOutside == 1){
					// put the node and the PR into the insideNodeComingEdgeList
					storeIncomingEdgePRIntoList(insideNodeComingEdgeList, destNode, curdestNodePR);
				}

				// edge comes from node outside the same block
				else{
					// put the dest nodeId and PR into the boundaryCondition
					storeIncomingEdgePRIntoList(boundaryCondition, destNode, curdestNodePR);
				}
			}
		}

		nodeNumberInBlock = graph.size();

		//inner loop, stop after running n iterations, n = innerIteration
		while(true){
			//iterate all the inside_block_node, and update their PR value, 
			//also add residual of each node

			totalResidual = updatePRValue(nextPR, previousPR, insideNodeComingEdgeList,boundaryCondition,innerIteration);
			//Works in the wa pf mapper!
			updateInsideNodeComingEdgeList(curBlockId, graph, nextPR, insideNodeComingEdgeList);
			//when everything get updated, time to increase innerIteration
			innerIteration+=1;
			
			if(nextPR == null){
				System.out.println("Check the nextPR which is null!");
				break;
			}

			//calculate average residual of the block
			double averageResidual=1;
			try{
				averageResidual = totalResidual/nodeNumberInBlock;
			}catch(ArithmeticException e){
				System.out.println("Check the nodeNumberInBlock, it may be 0 now");
			}
			
			if(averageResidual < Utils.EPSILON){
				//System.out.println("^^^^^^^^^Block ID: "+inputKey.toString()+"CONVERGED at "+innerIteration+" th inside iteration" );
				break;
			}
			else{
				// in the end of each iteration, set the averageResidual back to 0
				totalResidual = 0;
				
			}

		}

		//add the number of inside block iteration to counter
		context.getCounter(myCounter.ResidualCounter.OVERALL_INNER_BLOCK_ITERATION).increment(innerIteration);
		
		//get reported residual:[PR(start) - PR(end) / PR(start)]
		double reportedResidual = getBlockResidualForReport(previousPR, nextPR);
		long reportedResidualTransformed = (long)(reportedResidual*10000);
		context.getCounter(myCounter.ResidualCounter.RESIDUAL_SUM).increment(reportedResidualTransformed);
		
		//output two node PR for each block
		Configuration conf = context.getConfiguration();
		String param = conf.get("iterationNumber");
		int iterationNum = Integer.parseInt(param);
		outputPROfTwoNodesForEachBlock(nextPR, curBlockId, iterationNum);

		//reducer output
		emit(graph, nextPR, context);

	}

	//For inner iteration,there is no mapper, we need to generate input for reducer 
	// by ourselves
	public static void updateInsideNodeComingEdgeList(long curBlockId, HashMap<Long, String> graph,
		HashMap<Long, Double> nextPR,  HashMap<Long, List<Double> > insideNodeComingEdgeList){
		
		insideNodeComingEdgeList.clear();
		//I only care the inside-block edge
		Set<Long> nodeSet = new HashSet<Long>();
		nodeSet = graph.keySet();
		for(long nodeId : nodeSet){
			String[] destNodeIdStrArray = graph.get(nodeId).split(",");

			int numberOfDestNodes = destNodeIdStrArray.length;
			double curPR = nextPR.get(nodeId);

			for(String destNodeIdStr : destNodeIdStrArray){
				if(destNodeIdStr.equals("")) continue;
				long curDestNodeId = Long.parseLong(destNodeIdStr);
				long blockIdOfCurDestNode = BlockMatch.blockIDofNode(curDestNodeId);
				if(blockIdOfCurDestNode != curBlockId){
					//do nothing
				}
				else{
					List<Double> tempList = insideNodeComingEdgeList.get(curDestNodeId);
					if(tempList == null){
						tempList = new ArrayList<Double>();
						tempList.add( curPR/numberOfDestNodes);
						insideNodeComingEdgeList.put(curDestNodeId,tempList);
					}else{
						tempList.add( curPR/numberOfDestNodes);
					}
					
				}
			}

		}

	}

	public static void storeIncomingEdgePRIntoList(HashMap<Long, List<Double>> map, long nodeId, double curPR){
		
		if(!map.containsKey(nodeId)){
			List<Double> temp = new ArrayList<Double>();
			temp.add(curPR);
			map.put(nodeId, temp);
		}

		//get the List of PR and add curPR into it, then update the map
		else{
			List<Double> temp = new ArrayList<Double>();
			temp = map.get(nodeId);
			temp.add(curPR);
		}

	}

	public static void storeIncomingNodeIntoGraphAndUpdatePPR(HashMap<Long, Double> previousPR,
		HashMap<Long, String> graph, String[] info){
		long nodeId = Long.parseLong(info[0]);
		double prePROfNode = Double.parseDouble(info[1]);
		// store the node into the graph for reconstruction and output
		if(info.length == 2){
			graph.put(nodeId, "");
		}else if(info.length == 3){
			graph.put(nodeId, info[2]);
		}else{
			System.out.println("Check the input info!");
		}
		//store the nodeId and PPR into the previousPR map
		previousPR.put(nodeId, prePROfNode);
	}

	/** 
 	* Iterate all the inside block node and update their pagerank value
 	* 
 	*/
 	public static double updatePRValue(HashMap<Long, Double> nextPR, 
 		HashMap<Long, Double> previousPR, HashMap<Long, List<Double> > insideNodeComingEdgeList,
 		 HashMap<Long, List<Double> > boundaryCondition, long i){
 		double totalResidual = 0;
 		Set<Long> allNodesSet = new HashSet<Long>();
 		allNodesSet = previousPR.keySet();
 		for(Long curNodeId : allNodesSet){
 			List<Double> insideBlockEdgesPR = new ArrayList<Double>();
 			List<Double> outsideBlockEdgesPR = new ArrayList<Double>();
 			insideBlockEdgesPR = insideNodeComingEdgeList.get(curNodeId);
 			outsideBlockEdgesPR = boundaryCondition.get(curNodeId);
 			double curPR = 0;
			double prePR;

 			//for first iteration
 			if(i==0){
 				prePR = previousPR.get(curNodeId);
 			}
 			//for following iterations
 			else{
 				prePR = nextPR.get(curNodeId);
 			} 

 			
 			if(insideBlockEdgesPR != null){
 				for(double pr : insideBlockEdgesPR){
 					curPR += pr;
 				}
 			}
 			if(outsideBlockEdgesPR != null){
 				for(double pr : outsideBlockEdgesPR){
 					curPR += pr;
 				}
 			}
 			//update nextPR
 			double finalCurPR = (1-Utils.DAMPING_FACTOR)/Utils.N + Utils.DAMPING_FACTOR*(curPR);
 			nextPR.put(curNodeId, finalCurPR);

 			//calculate residual and add it to reducer scope residual
 			double curResidual = Math.abs(finalCurPR-prePR)/finalCurPR;
 			totalResidual+=curResidual;
 		}
 		return totalResidual;
 	}
	
	public static double getBlockResidualForReport(HashMap<Long, Double> previousPR, 
		HashMap<Long, Double> nextPR){
		double reportedResidual = 0;
		Set<Long> nodeIdSet = new HashSet<Long>();
		nodeIdSet = previousPR.keySet();
		for(Long nodeId : nodeIdSet){
			double prStart = previousPR.get(nodeId);
			if(nextPR == null){
				System.out.println("******************nextPR is null!!!");
			}
			if(nextPR.get(nodeId) == null){
				System.out.println("****************didn't find node in npr");
			}
			double prEnd = nextPR.get(nodeId);
			double curResidual = Math.abs( (prStart-prEnd) )/prEnd;
			reportedResidual+=curResidual;
		}
		return reportedResidual;

	}

	

	/*
	* Output PR value of two nodes with lowest nodeID number for each block
	* @Param nextPR : the hashMap that contains the pair of nodeID and Current PageRank value
	*/
	public static void outputPROfTwoNodesForEachBlock(HashMap<Long, Double> nextPR, long curBlockId, int iterNum){

		//getSmallest nodeID by blockID
		long smallestNodeId = BlockMatch.getSmallestNodeIdByBlockId(curBlockId);
		long secondNodeId = smallestNodeId+1;
		double smallestNodePR = 0;
		double secondSmallestNodePR = 0;
		//

		if(nextPR.get(smallestNodeId)!=null){
			smallestNodePR = nextPR.get(smallestNodeId);
		}else{
			System.out.println("No node info found in NPR in reducer!");
			return;
		}

		if(nextPR.get(secondNodeId) != null){
			secondSmallestNodePR = nextPR.get(secondNodeId);
		}else{
			System.out.println("No node info found in NPR in reducer!");
			return;
		}
		System.out.println("The PageRank value of Node "+smallestNodeId+" in Block "+curBlockId+" in "+iterNum+" iteration is: "+smallestNodePR);
		System.out.println("The PageRank value of Node "+secondNodeId+" in Block "+curBlockId+" in "+iterNum+" iteration is: "+secondSmallestNodePR);

	}

	/*
	 * write the result of reducer to txt files for next round of iteration
	*/
	public static void emit( HashMap<Long, String> graph, HashMap<Long, Double> nextPR,
		Context context)throws IOException, InterruptedException{
		Set<Long> nodeIdSet = new HashSet<Long>();
		nodeIdSet = graph.keySet();
		for(long nodeId : nodeIdSet){
			double curPR = nextPR.get(nodeId);
			String edgeList = graph.get(nodeId);
			String outputValue = nodeId+" "+curPR+" "+edgeList;
			context.write(null, new Text(outputValue));
		}
	}

}