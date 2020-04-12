package com.sitech.mr.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	private FlowBean key_out = new FlowBean();
	private Text value_out =new Text();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		String[] words = value.toString().split("\t");
		
		value_out.set(words[0]);
		long upFlow = Long.parseLong(words[(words.length-3)]);
		long downFlow = Long.parseLong(words[(words.length-2)]);
		long sumFlow = Long.parseLong(words[(words.length-1)]);

		key_out.setUpFlow(upFlow);
		key_out.setDownFlow(downFlow);
		key_out.setSumFlow(sumFlow);

		
		context.write(key_out, value_out);
		
	}
	
}
