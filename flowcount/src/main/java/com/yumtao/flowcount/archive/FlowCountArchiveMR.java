package com.yumtao.flowcount.archive;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * mapper 输出压缩/reducer 输出压缩
 * mapper 输出压缩： TODO 暂时存在问题，待学习
 * 
 * reducer输出压缩：
 * 1: driver 类中,添加如下代码
 * FileOutputFormat.setCompressOutput(job, true);
 * FileOutputFormat.setOutputCompressorClass(job, Class<? extends CompressionCodec>);
 * 
 * 2: 配置：
 * conf.set("mapreduce.output.fileoutputformat.compress", "true");		
 * conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
 * @author yumTao
 *
 */
public class FlowCountArchiveMR {

	static class FlowCountArchiveMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			/**
			 * value:1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
			 */
			String line = value.toString();
			List<String> msgs = Arrays.asList(line.split("\t"));
			String phone = msgs.get(1);
			long upFlow = Long.valueOf(msgs.get(8));
			long downFlow = Long.valueOf(msgs.get(9));
			long totalFlow = upFlow + downFlow;
			context.write(new Text(phone), new Text(upFlow + "," + downFlow + "," + totalFlow));
		}

	}

	static class FlowCountArchiveReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			long upFlow = 0l;
			long downFlow = 0l;
			long totalFlow = 0l;

			for (Text text : value) {
				String flowStr = text.toString();
				List<String> flowGroup = Arrays.asList(flowStr.split(","));
				upFlow += Long.valueOf(flowGroup.get(0));
				downFlow += Long.valueOf(flowGroup.get(1));
				totalFlow += Long.valueOf(flowGroup.get(2));
			}

			String flowResult = String.format("upFlow=%d\tdownFlow=%d\ttotalFlow=%d", upFlow, downFlow, totalFlow);
			context.write(new Text(key), new Text(flowResult));
		}
	}

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
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");
		
		// map set out archive by code
//		conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
//		conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);

		
		// reduce set out archive
//		conf.set("mapreduce.output.fileoutputformat.compress", "true");
//		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");

		
		Job archiveJob = Job.getInstance(conf);

		archiveJob.setJarByClass(FlowCountArchiveMR.class);

		archiveJob.setMapperClass(FlowCountArchiveMapper.class);
		archiveJob.setReducerClass(FlowCountArchiveReducer.class);

		archiveJob.setMapOutputKeyClass(Text.class);
		archiveJob.setMapOutputValueClass(Text.class);

		archiveJob.setOutputKeyClass(Text.class);
		archiveJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(archiveJob, new Path("D:/tmp/mr/flow.log"));
		
		FileOutputFormat.setOutputPath(archiveJob, new Path("D:/tmp/mr/out_archive/"));
		// 设置reduce输出压缩
//		FileOutputFormat.setCompressOutput(archiveJob, true);
//		FileOutputFormat.setOutputCompressorClass(archiveJob, GzipCodec.class);


		boolean flag = archiveJob.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}
}
