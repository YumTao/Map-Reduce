package com.yumtao.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//把这个描述好的job提交给集群去运行
public class WordCountRunner {
	public static void main(String[] args) throws Exception {

		// 获取job实例
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "master");
		Job wcJob = Job.getInstance(conf);
		// 指定job所在的jar包
		// wcJob.setJar("D:/wordcount/wordcount.jar");
		wcJob.setJarByClass(WordCountRunner.class);

		// 指定map&reduce执行程序
		wcJob.setMapperClass(WordCountMapper.class);
		wcJob.setReducerClass(WordCountReduce.class);

		wcJob.setMapOutputKeyClass(Text.class);
		wcJob.setMapOutputValueClass(IntWritable.class);

		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(wcJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(wcJob, new Path(args[1]));

		boolean flag = wcJob.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}
}
