package com.sitech.mr.sort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

	
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		
		for (Text value : values) {
			context.write(value, key);

		}
	}
	

}
