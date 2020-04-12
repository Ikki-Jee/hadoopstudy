package com.sitech.mr.reducejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReduceJoinDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ReduceJoinDriver.class);
		job.setJobName("ReduceJoinDriver");
		
		FileSystem fs = FileSystem.get(conf);
		
		Path inputPath = new Path("/Users/ikki/Desktop/hadooptest/reducejoin/input");
		Path outputPath = new Path("/Users/ikki/Desktop/hadooptest/reducejoin/output");
		
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);			
		}

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(ReduceJoinMapper.class);
		job.setReducerClass(ReduceJoinReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinBean.class);
	
		job.setOutputKeyClass(JoinBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
