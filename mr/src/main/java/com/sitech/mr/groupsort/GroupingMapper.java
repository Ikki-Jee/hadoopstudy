package com.sitech.mr.groupsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupingMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
	OrderBean ob = new OrderBean();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] words = value.toString().split("\t");
		
		ob.setOrder_id(Integer.parseInt(words[0]));
		ob.setPrice(Double.parseDouble(words[2]));
		
		context.write(ob, NullWritable.get());
	}
	

}
