package com.sitech.mr.partitions;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

	private FlowBean value_out = new FlowBean();
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		
		long sumUpFlow =0;
		long sumDownFlow =0;
		
		for (FlowBean flowBean : values) {
			sumUpFlow+=flowBean.getUpFlow();
			sumDownFlow+=flowBean.getDownFlow();
		}
		
		value_out.setUpFlow(sumUpFlow);
		value_out.setDownFlow(sumDownFlow);
		value_out.setSumFlow(sumDownFlow+sumUpFlow);

		context.write(key, value_out);
	}
	

}
