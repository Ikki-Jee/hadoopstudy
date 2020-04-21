package com.sitech.mr.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class IndexDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		Path inputPath = new Path("/Users/ikki/Desktop/hadooptest/search_index/inputdata");
		Path outputPath = new Path("/Users/ikki/Desktop/hadooptest/search_index/output");
		Path finalOutputPath = new Path("/Users/ikki/Desktop/hadooptest/search_index/output2");

		
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);			
		}
		if(fs.exists(finalOutputPath)) {
			fs.delete(finalOutputPath, true);			
		}
		
		
		Job job1 = Job.getInstance(conf);
		
		job1.setJarByClass(IndexDriver.class);
		job1.setJobName("index1");
		FileInputFormat.setInputPaths(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, outputPath);		
		job1.setMapperClass(IndexMapper.class);
		job1.setReducerClass(IndexReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
		
	//------------------------------------
		Configuration conf2 = new Configuration();
		conf2.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "_");
		Job job2 = Job.getInstance(conf2);
		

		
		job2.setJarByClass(IndexDriver.class);
		job2.setJobName("index2");
		

		FileInputFormat.setInputPaths(job2, outputPath);
		FileOutputFormat.setOutputPath(job2, finalOutputPath);		
		job2.setMapperClass(IndexMapper2.class);
		job2.setReducerClass(IndexReducer2.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		
		//如果conf2.set()且在new Job()之后，会失效；conf2必须在new Job()前
//		job2.getConfiguration().set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "_");
		job2.setInputFormatClass(KeyValueTextInputFormat.class);


		
		ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
		controlledJob2.addDependingJob(controlledJob1);
		
		JobControl jobControl = new JobControl("index");

		jobControl.addJob(controlledJob1);
		jobControl.addJob(controlledJob2);
		
		jobControl.run();
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
