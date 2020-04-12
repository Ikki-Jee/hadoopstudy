package com.sitech.mr.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestSequenceReader {

	@Test
	public void TestSR() {
		Path filePath = new Path("/Users/ikki/Desktop/output/part-r-00000");
		Configuration configuration = new Configuration();
		try {
			FileSystem fs =FileSystem.get(configuration);
			SequenceFile.Reader reader;
			
				reader = new SequenceFile.Reader(fs, filePath, configuration);
			
			Text key =new Text();
			BytesWritable val = new BytesWritable();
			while (reader.next(key, val)) {
				System.out.println(key.toString());
				System.out.println(new String(val.getBytes()));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
