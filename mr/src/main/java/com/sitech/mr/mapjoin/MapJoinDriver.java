package com.sitech.mr.mapjoin;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapJoinDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MapJoinDriver.class);
		job.setJobName("MapJoinDriver");
		
		FileSystem fs = FileSystem.get(conf);
		
		Path inputPath = new Path("/Users/ikki/Desktop/hadooptest/mapjoin/input/order.txt");

		Path outputPath = new Path("/Users/ikki/Desktop/hadooptest/mapjoin/output");
		
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);			
		}

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(MapJoinMapper.class);
		
		job.addCacheFile(URI.create("file:///Users/ikki/Desktop/hadooptest/mapjoin/input/pd.txt"));;
		
		job.setMapOutputKeyClass(JoinBean.class);
		job.setMapOutputValueClass(NullWritable.class);
	
		job.setNumReduceTasks(0);
//		job.setOutputKeyClass(JoinBean.class);
//		job.setOutputValueClass(NullWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
