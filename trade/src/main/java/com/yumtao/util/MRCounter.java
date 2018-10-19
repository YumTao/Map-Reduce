package com.yumtao.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MRCounter {
	private static final Logger log = LoggerFactory.getLogger(MRCounter.class);

	static class MRCounterMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.getCounter("com.yumtao", "testcount").increment(1);
			Counter counter = context.getCounter("com.yumtao", "testcount");
			long countValue = counter.getValue();
			log.debug("count value : {}", countValue);
			context.write(value, value);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf);
		job.setJarByClass(MRCounter.class);
		
		job.setMapperClass(MRCounterMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 这里是我们正常的需要处理的数据所在路径
		FileInputFormat.setInputPaths(job, new Path("D:/tmp/mr/trade/trade_records.txt"));
		FileOutputFormat.setOutputPath(job, new Path("D:/tmp/mr/trade/output_counter"));

		// 不需要reducer
		job.setNumReduceTasks(0);
		boolean flag = job.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}

}
