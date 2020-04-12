package com.sitech.mr.myoutformat;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

	private FileSystem fs;
	private Path firstPath = new Path("/Users/ikki/Desktop/hadooptest/outputformat/outputdata/firstdata.txt");
	private Path otherPath = new Path("/Users/ikki/Desktop/hadooptest/outputformat/outputdata/otherdata.txt");
	
	private OutputStream firstOStream;
	private OutputStream otherOStream;


	
	public MyRecordWriter(TaskAttemptContext job) throws IOException {
		Configuration conf = job.getConfiguration();
		fs = FileSystem.get(conf);
		
		firstOStream = fs.create(firstPath, true);
		otherOStream = fs.create(otherPath, true);

	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(key.toString().contains("sitech")) {
			firstOStream.write(key.toString().getBytes());
			
		}else {
			otherOStream.write(key.toString().getBytes());
		}
		

	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {

		if (firstOStream != null) {
			IOUtils.closeStream(firstOStream);
			IOUtils.closeStream(otherOStream);
		}
	}

}
