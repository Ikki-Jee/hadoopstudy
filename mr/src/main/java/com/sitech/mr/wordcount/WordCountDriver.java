package com.sitech.mr.wordcount;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration conf = new Configuration();
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "hadoop103");
//		conf.set("fs.defaultFS","hdfs://hadoop102:9000");
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCountDriver.class);
		job.setJobName("Word Count");
		FileSystem fs =FileSystem.get(conf);
//		FileSystem fs =FileSystem.get(URI.create("hdfs://hadoop102:9000"), conf,"hadoop");
		Path inputPath = new Path("/Users/ikki/Desktop/hadooptest/wordcount/wordcounttest.txt");
		Path outputPath = new Path("/Users/ikki/Desktop/hadooptest/wordcount/wordcountoutput");
//		Path outputPath = new Path("hdfs://hadoop102:9000/wordcountoutput");
		
		if(fs.exists(outputPath)) {fs.delete(outputPath,true);}
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setCombinerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
