package com.sitech.mr.index;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper2 extends Mapper<Text, Text, Text, Text>{
	


	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

			context.write(key, value);
	
		
	}
	
	

}
