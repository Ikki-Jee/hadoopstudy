package com.sitech.mr.mapjoin;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class MapJoinMapper extends Mapper<LongWritable, Text, JoinBean, NullWritable>{
	private Map<String, String> pdata= new HashMap<String, String>();
	private JoinBean key_out = new JoinBean();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, JoinBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		URI[] uris = context.getCacheFiles();
		
		for (URI uri : uris) {
			FSDataInputStream iStream = FileSystem.get(context.getConfiguration()).open(new Path(uri));
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(iStream, "utf-8"));
			
			String line =null;
			
			while (StringUtils.isNotEmpty(line=reader.readLine())) {
				
				String[] words = line.split("\t");
				pdata.put(words[0], words[1]);
				
				
			}
			
		}

	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, JoinBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String words[] = value.toString().split("\t");
		
		long orderId = Long.parseLong(words[0]);
		int amount = Integer.parseInt(words[2]);
		
		key_out.setAmount(amount);
		key_out.setOrderId(orderId);
		key_out.setPname(pdata.get(words[1]));
		
		context.write(key_out, NullWritable.get());
	
	}


	
	

}
