package com.sitech.mr.flowcount;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable, Comparable<FlowBean> {

	private long upFlow;
	private long downFlow;
	private long sumFlow;
	
	
	public FlowBean() {
		super();
	}
	
	

	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow+downFlow;
	}



	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();

	}

	@Override
	public int compareTo(FlowBean o) {
		// TODO Auto-generated method stub
		return this.sumFlow > o.getSumFlow() ? -1 : 1;
	}



	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}
	
	
//	public static void main(String[] args) throws IOException {
//		String temString =null;
//		File file = new File("/Users/ikki/Desktop/inputflow.txt");
//		FileInputStream fis = new FileInputStream(file);
//		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
//		while((temString=reader.readLine())!=null) {
//			System.out.println(temString);
//			String[] words = temString.split("\t");
//			System.out.println(words[1]);
//			System.out.println(words[(words.length-3)]);
//			System.out.println(words[(words.length-2)]);
//			
//		}
//		
//		
//	}
}
