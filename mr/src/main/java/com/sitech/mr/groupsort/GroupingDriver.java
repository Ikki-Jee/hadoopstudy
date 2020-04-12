package com.sitech.mr.groupsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupingDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		FileSystem fs =FileSystem.get(conf);
		Path inputPath = new Path("/Users/ikki/Desktop/hadooptest/groupsort/input/orderdata.txt");
		Path outputPath = new Path("/Users/ikki/Desktop/hadooptest/groupsort/output");

		if(fs.exists(outputPath)) {fs.delete(outputPath,true);}

		job.setJarByClass(GroupingDriver.class);
		
		job.setMapperClass(GroupingMapper.class);
		job.setReducerClass(GroupingReducer.class);
		
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
//		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setGroupingComparatorClass(GroupingComparator.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
