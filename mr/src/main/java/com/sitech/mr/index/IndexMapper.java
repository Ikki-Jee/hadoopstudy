package com.sitech.mr.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private String fileName; 
	private Text key_out = new Text();
	private IntWritable value_out = new IntWritable(1);
	
	//在setup方法中获取文件名，就运行一次，不然在map方法中要循环很多次
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		//文件需要从切片中获取，从context中获取输入切片，再进行类型强转得到文件切片，使用获取路径方法返回路径，再获取文件名
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		fileName = fileSplit.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String[] words = value.toString().split("\t");
		
		for (String word : words) {
			key_out.set(word+"_"+fileName);
			context.write(key_out, value_out);
		}
		
	}
	
	

}
