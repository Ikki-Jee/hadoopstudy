package com.sitech.mr.partitions;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, FlowBean> {

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		// TODO Auto-generated method stub
		
		String perPhoneNum = key.toString().substring(0, 3);
		
		int partition = numPartitions-5;
		
		switch (perPhoneNum) {
		case "136":
			partition =  numPartitions-1;
			break;
		case "137":
			partition =  numPartitions-2;
			break;
		case "138":
			partition =  numPartitions-3;
			break;
		case "139":
			partition =  numPartitions-4;
			break;
		default:
			break;
		} 
		
		return partition;
	}

}
