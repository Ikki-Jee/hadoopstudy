package com.sitech.mr.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	private IntWritable value_out = new IntWritable();	
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		
		for (IntWritable intWritable : values) {
			sum += intWritable.get();
		}
		value_out.set(sum);
		context.write(key, value_out);

	}
	
	

}
