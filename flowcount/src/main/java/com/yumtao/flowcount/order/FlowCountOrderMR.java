package com.yumtao.flowcount.order;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountOrderMR {

	class FlowCountOrderMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// line: 13480253104 upFlow=180 downFlow=180 totalFlow=360
			String line = value.toString();
			List<String> flowMsg = Arrays.asList(line.split("\t"));
			String phone = flowMsg.get(0);
			long totalFlow = Long.valueOf(Arrays.asList(flowMsg.get(3).split("=")).get(1));

		}

	}

	class FlowCountOrderReducer extends Reducer<LongWritable, Text, Text, Text> {

		@Override
		protected void reduce(LongWritable arg0, Iterable<Text> arg1,
				Reducer<LongWritable, Text, Text, Text>.Context arg2) throws IOException, InterruptedException {
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "singlenode");
		Job orderJob = Job.getInstance(conf);
		orderJob.setJarByClass(FlowCountOrderMR.class);

		orderJob.setMapperClass(FlowCountOrderMapper.class);
		orderJob.setReducerClass(FlowCountOrderReducer.class);

		orderJob.setMapOutputKeyClass(LongWritable.class);
		orderJob.setMapOutputValueClass(Text.class);

		orderJob.setOutputKeyClass(Text.class);
		orderJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(orderJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(orderJob, new Path(args[1]));

		boolean flag = orderJob.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}

}
