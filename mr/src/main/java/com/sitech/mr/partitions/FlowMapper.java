package com.sitech.mr.partitions;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	private Text key_out = new Text();
	private FlowBean value_out =new FlowBean();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		String[] words = value.toString().split("\t");
		
		key_out.set(words[1]);
		long upFlow = Long.parseLong(words[(words.length-3)]);
		long downFlow = Long.parseLong(words[(words.length-2)]);
		value_out.setUpFlow(upFlow);
		value_out.setDownFlow(downFlow);
		
		context.write(key_out, value_out);
		
	}
	
}
