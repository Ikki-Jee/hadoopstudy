package com.sitech.mr.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowDriver.class);
		job.setJobName("flowcount");
		
		FileSystem fs = FileSystem.get(conf);
		
		Path inputPath = new Path("/Users/ikki/Desktop/hadooptest/sort/input/sorted_inputdata");
		Path outputPath = new Path("/Users/ikki/Desktop/hadooptest/sort/output");
		
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);			
		}

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		 
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(5);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		
	}

}
