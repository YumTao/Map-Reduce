package com.yumtao.component.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogEnhancerOutputFormat extends FileOutputFormat<Text, NullWritable>{

	
	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path enhancePath = new Path("hdfs://hdp-node01:9000/flow/enhancelog/enhanced.log");
		Path toCrawlPath = new Path("hdfs://hdp-node01:9000/flow/tocrawl/tocrawl.log");
		
		// FSDataOutputStream： 这里用于HDFS文件的读写
		FSDataOutputStream enhanceOut = fs.create(enhancePath);
		FSDataOutputStream toCrawlOut = fs.create(toCrawlPath);
		
		
		return new MyRecordWriter(enhanceOut,toCrawlOut);
	}
	
	
	
	static class MyRecordWriter extends RecordWriter<Text, NullWritable>{
		
		FSDataOutputStream enhanceOut = null;
		FSDataOutputStream toCrawlOut = null;
		
		public MyRecordWriter(FSDataOutputStream enhanceOut, FSDataOutputStream toCrawlOut) {
			this.enhanceOut = enhanceOut;
			this.toCrawlOut = toCrawlOut;
		}

		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			 
			//有了数据，你来负责写到目的地  —— hdfs
			//判断，进来内容如果是带tocrawl的，就往待爬清单输出流中写 toCrawlOut
			if(key.toString().contains("tocrawl")){
				toCrawlOut.write(key.toString().getBytes());
			}else{
				enhanceOut.write(key.toString().getBytes());
			}
				
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			 
			if(toCrawlOut!=null){
				toCrawlOut.close();
			}
			if(enhanceOut!=null){
				enhanceOut.close();
			}
			
		}
		
		
	}
}
