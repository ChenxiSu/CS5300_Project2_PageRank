import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.lang.String;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.nio.charset.Charset;

public class JobRunner {

	//create a mapreduce job to calculate page rank of each node
	// in the whole graph
	public static Job createJob(int jobId, String inputDirectory, String outputDirectory)throws IOException {
		//Configuration conf = new Configuration();
		Job newJob = Job.getInstance();
		newJob.setJarByClass(GraphMapper.class);
		newJob.setMapperClass(GraphMapper.class);
		newJob.setMapOutputKeyClass(LongWritable.class);
		newJob.setMapOutputValueClass(Text.class);

		newJob.setReducerClass(GraphReducer.class);
		newJob.setOutputKeyClass(Text.class);
		newJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(newJob, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(newJob, new Path(outputDirectory));
        newJob.setInputFormatClass(KeyValueTextInputFormat.class);
        return newJob;
	}



}
