package com.yumtao.flowcount.mypartition;

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

import com.yumtao.flowcount.mypartition.vo.FlowAndPhoneVo;

/**
 * 自定义partitioner, 自实现key分组, 实现多文件输出。 
 * 步骤：
 * 1.创建自定义class,继承Partitioner,重写getPartition方法。
 * 2.driver应用自定义partitioner,以及设置对应reducetask数量（注意：reducetask数必须与分区数一致）
 * 
 * @author yumTao
 *
 */
public class FlowCountPartitionMR {

	// 13502468823 upFlow=7335 downFlow=110349 totalFlow=117684
	static class FlowCountPartitionMapper extends Mapper<LongWritable, Text, FlowAndPhoneVo, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, FlowAndPhoneVo, Text>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			List<String> flowMsg = Arrays.asList(line.split("\t"));
			String phone = flowMsg.get(0);

			long upFlow = Long.valueOf(Arrays.asList(flowMsg.get(1).split("=")).get(1));
			long downFlow = Long.valueOf(Arrays.asList(flowMsg.get(2).split("=")).get(1));
			long totalFlow = Long.valueOf(Arrays.asList(flowMsg.get(3).split("=")).get(1));
			FlowAndPhoneVo vo = new FlowAndPhoneVo(upFlow, downFlow, totalFlow, phone);

			context.write(vo, new Text(line));
		}

	}

	static class FlowCountPartitionReducer extends Reducer<FlowAndPhoneVo, Text, Text, Text> {

		@Override
		protected void reduce(FlowAndPhoneVo key, Iterable<Text> value,
				Reducer<FlowAndPhoneVo, Text, Text, Text>.Context context) throws IOException, InterruptedException {

			System.out.println(String.format("once reduce oper: key=%s", key.toString()));
			for (Text text : value) {
				context.write(new Text(key.getPhone()), new Text(
						text.toString().substring(text.toString().indexOf("\t") + 1, text.toString().length())));
			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		Job myPartitionJob = Job.getInstance(conf);
		myPartitionJob.setJarByClass(FlowCountPartitionMR.class);

		myPartitionJob.setMapperClass(FlowCountPartitionMapper.class);
		myPartitionJob.setReducerClass(FlowCountPartitionReducer.class);

		myPartitionJob.setMapOutputKeyClass(FlowAndPhoneVo.class);
		myPartitionJob.setMapOutputValueClass(Text.class);

		myPartitionJob.setOutputKeyClass(Text.class);
		myPartitionJob.setOutputValueClass(Text.class);

		myPartitionJob.setPartitionerClass(PhonePartition.class);
		myPartitionJob.setNumReduceTasks(6);

		FileInputFormat.setInputPaths(myPartitionJob, new Path("D:/tmp/mr/out_order/part-r-00000"));
		FileOutputFormat.setOutputPath(myPartitionJob, new Path("D:/tmp/mr/out_part_by_user/"));

		boolean flag = myPartitionJob.waitForCompletion(true);
		System.exit(flag ? 0 : 1);

	}

}
