package com.yumtao.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountRunner {

	/**
	 * MR程序本地运行，支持debug模式，设置conf.set("mapreduce.framework.name", "local")即可
	 * windows下，需将对应HADOOP_HOME对应的hadoop替换成windows编译的版本
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");
		Job flJob = Job.getInstance(conf);

		flJob.setJarByClass(FlowCountRunner.class);

		flJob.setMapperClass(FlowCountMapper.class);
		flJob.setReducerClass(FlowCountReducer.class);

		flJob.setMapOutputKeyClass(Text.class);
		flJob.setMapOutputValueClass(Text.class);

		flJob.setOutputKeyClass(Text.class);
		flJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(flJob, new Path("D:/tmp/mr/flow.log"));
		FileOutputFormat.setOutputPath(flJob, new Path("D:/tmp/mr/out/"));

		boolean flag = flJob.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}

}
