package com.yumtao.clickflow.step2;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yumtao.clickflow.step1.Step1;
import com.yumtao.clickflow.util.DateUtil;
import com.yumtao.clickflow.vo.AccessMsgByStep;

public class Step2 {
	private static final Logger log = LoggerFactory.getLogger(Step1.class);

	static class Step2Mapper extends Mapper<LongWritable, Text, AccessMsgByStep, AccessMsgByStep> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, AccessMsgByStep, AccessMsgByStep>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] split = line.split("\t");
			String timestamp = split[0];
			String ip = split[1];
			String cookie = split[2];
			String session = split[3];
			String url = split[4];
			String referal = split[5];
			AccessMsgByStep msg = new AccessMsgByStep(timestamp, ip, cookie, session, url, referal);
			context.write(msg, msg);
			log.debug("map write value= {}", msg.toString());
		}

	}

	static class Step2Reducer extends Reducer<AccessMsgByStep, AccessMsgByStep, AccessMsgByStep, NullWritable> {

		private final static long DEFAULT_STAYTIME = 30;

		@Override
		protected void reduce(AccessMsgByStep key, Iterable<AccessMsgByStep> msgs,
				Reducer<AccessMsgByStep, AccessMsgByStep, AccessMsgByStep, NullWritable>.Context context)
				throws IOException, InterruptedException {
			int step = 1;
			AccessMsgByStep old = null;
			for (AccessMsgByStep now : msgs) {
				now.setStep(step++);
				now.setStayTime(DEFAULT_STAYTIME); // 设置默认停留时间
				if (null != old) {
					// 更正上一个停留时间（new - old）
					old.setStayTime(DateUtil.getMillSecBetween(old.getTime(), now.getTime()));
					// 写出上一个
					context.write(old, NullWritable.get());
				}
				old = now;
			}
			context.write(old, NullWritable.get());
		}

	}

	public static void main(String[] args) throws Exception {
		File descDir = new File("D:/tmp/mr/click_flow/step2");
		FileUtils.forceDeleteOnExit(descDir);

		Configuration conf = new Configuration();
		Job step2 = Job.getInstance(conf);
		conf.set("mapreduce.framework.name", "local");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");

		step2.setJarByClass(Step1.class);

		step2.setGroupingComparatorClass(SessionGroup.class);
		
		step2.setMapperClass(Step2Mapper.class);
		step2.setReducerClass(Step2Reducer.class);

		step2.setMapOutputKeyClass(AccessMsgByStep.class);
		step2.setMapOutputValueClass(AccessMsgByStep.class);
		step2.setOutputKeyClass(AccessMsgByStep.class);
		step2.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(step2, new Path("D:/tmp/mr/click_flow/step1/part-r-00000"));
		FileOutputFormat.setOutputPath(step2, new Path("D:/tmp/mr/click_flow/step2"));

		boolean flag = step2.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}

}
