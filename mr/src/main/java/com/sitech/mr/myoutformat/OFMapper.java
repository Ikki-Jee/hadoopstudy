package com.sitech.mr.myoutformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OFMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	Text sperator = new Text("\t");
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		value.set(value.toString()+"\r");
		
		context.write(value, NullWritable.get());
	}
	
	

}
