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

import com.yumtao.flowcount.vo.FlowVo;

/**
 * 1.对流量统计后的数据按总流量进行倒序排列
 * 2.实现思想：MR程序在partition时，会根据key进行排序，所以map任务输出只要key能根据总流量进行倒序排列即可
 * 
 * @author yumTao
 *
 */
public class FlowCountOrderMR {

	/**
	 * notice内部类必须添加static修饰，否则mr程序执行不正常
	 * 
	 * @author yumTao
	 *
	 */
	static class FlowCountOrderMapper extends Mapper<LongWritable, Text, FlowVo, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowVo, Text>.Context context)
				throws IOException, InterruptedException {
			// line: 13480253104 upFlow=180 downFlow=180 totalFlow=360
			String line = value.toString();
			List<String> flowMsg = Arrays.asList(line.split("\t"));
			String phone = flowMsg.get(0);

			long upFlow = Long.valueOf(Arrays.asList(flowMsg.get(1).split("=")).get(1));
			long downFlow = Long.valueOf(Arrays.asList(flowMsg.get(2).split("=")).get(1));
			long totalFlow = Long.valueOf(Arrays.asList(flowMsg.get(3).split("=")).get(1));
			FlowVo flowVo = new FlowVo(upFlow, downFlow, totalFlow);

			context.write(flowVo, new Text(phone));
		}

	}

	static class FlowCountOrderReducer extends Reducer<FlowVo, Text, Text, Text> {

		@Override
		protected void reduce(FlowVo key, Iterable<Text> phones, Reducer<FlowVo, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			for (Text phone : phones) {
				context.write(phone, new Text(String.format("upFlow=%d\tdownFlow=%d\ttotalFlow=%d", key.getUpFlow(),
						key.getDownFlow(), key.getTotalFlow())));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");
		Job orderJob = Job.getInstance(conf);
		orderJob.setJarByClass(FlowCountOrderMR.class);

		orderJob.setMapperClass(FlowCountOrderMapper.class);
		orderJob.setReducerClass(FlowCountOrderReducer.class);

		orderJob.setMapOutputKeyClass(FlowVo.class);
		orderJob.setMapOutputValueClass(Text.class);

		orderJob.setOutputKeyClass(Text.class);
		orderJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(orderJob, new Path("D:/tmp/mr/out/part-r-00000"));
		FileOutputFormat.setOutputPath(orderJob, new Path("D:/tmp/mr/out_order"));

		boolean flag = orderJob.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}

}
