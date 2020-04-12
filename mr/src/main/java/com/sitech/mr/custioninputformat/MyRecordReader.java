package com.sitech.mr.custioninputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyRecordReader extends RecordReader<Text, BytesWritable> {
	
	private Text key;
	private BytesWritable value;
	
	private FileSplit fileSplit;
	private FileSystem fs;
	private FSDataInputStream open;
	private boolean flag=true;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		key = new Text();
		value = new BytesWritable();
		fileSplit = (FileSplit) split;		
		fs=FileSystem.get(context.getConfiguration());
		
		open =fs.open(fileSplit.getPath() );
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if(flag) {
			key.set(fileSplit.getPath().getName());
			byte [] data = new byte[(int)fileSplit.getLength()];
			
			IOUtils.readFully(open, data, 0, data.length);
			
			value.set(data, 0, data.length);
			flag = false;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		fs.close();

	}

}
