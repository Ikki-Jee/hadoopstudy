package com.sitech.mr.remote.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable value_out = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		int sum = 0;
		Iterator<IntWritable> iterator = values.iterator();
		
		while (iterator.hasNext()) {
			IntWritable value = (IntWritable) iterator.next();
			sum+=value.get();
			
		}
		value_out.set(sum);
		context.write(key, value_out);
	
	}

	
	
}
