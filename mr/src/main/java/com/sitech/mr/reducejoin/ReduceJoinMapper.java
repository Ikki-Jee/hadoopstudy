package com.sitech.mr.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, JoinBean>{
	
	private Text key_out = new Text();
	private JoinBean value_out = new JoinBean();
	
	private String fileName;
	
	
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, JoinBean>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit fSplit = (FileSplit) context.getInputSplit();
		fileName=fSplit.getPath().getName();
	}
	
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		key_out.set(fileName);
		
		if(fileName.contains("order")) {
			String[] wordStrings = value.toString().split("\t");
			
			long orderId = Long.parseLong(wordStrings[0]);
			int amount = Integer.parseInt(wordStrings[2]);
			
			value_out.setOrderId(orderId);
			value_out.setAmount(amount);
			value_out.setPid(wordStrings[1]);
			value_out.setPname(" ");
		}else {
			String[] wordStrings = value.toString().split("\t");
			
			value_out.setPname(wordStrings[1]);
			value_out.setPid(wordStrings[0]);
		}
	
		context.write(key_out, value_out);
	}

	
	

}
