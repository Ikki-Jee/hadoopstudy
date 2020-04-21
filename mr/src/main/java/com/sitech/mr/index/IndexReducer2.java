package com.sitech.mr.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer2 extends Reducer<Text, Text, Text, Text>{

	private Text value_out = new Text();	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		
		StringBuilder sb = new StringBuilder();

        // 1 拼接
		for (Text value : values) {
			sb.append(value.toString().replace("\t", "-->") + "\t");
		}

		value_out.set(sb.toString());

		// 2 写出
		context.write(key, value_out);


	}
	
	

}
