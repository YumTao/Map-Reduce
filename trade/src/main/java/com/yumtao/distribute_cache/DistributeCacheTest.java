package com.yumtao.distribute_cache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * TODO 待完善
 * @author yumTao
 *
 */
public class DistributeCacheTest {
	private static final Logger log = LoggerFactory.getLogger(DistributeCacheTest.class);

	static class DistributeCacheMapper extends Mapper<LongWritable, Text, Text, Text> {

		FileReader in = null;
		BufferedReader reader = null;
		Map<String, String> b_tab = new HashMap<String, String>();
		String localpath = null;
		String uirpath = null;

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.map(key, value, context);
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 通过这几句代码可以获取到cache file的本地绝对路径，测试验证用
			URI[] cacheFiles = context.getCacheFiles();
			for (URI uri : cacheFiles) {
				log.debug("cacheFile path : {}", uri.getPath());
			}

			// 缓存文件的用法——直接用本地IO来读取
			// 这里读的数据是map task所在机器本地工作目录中的一个小文件
			/*
			 * in = new FileReader("b.txt"); reader = new BufferedReader(in); String line =
			 * null; while (null != (line = reader.readLine())) {
			 * 
			 * String[] fields = line.split(","); b_tab.put(fields[0], fields[1]);
			 * 
			 * } IOUtils.closeStream(reader); IOUtils.closeStream(in);
			 */
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf);

		job.setJarByClass(DistributeCacheTest.class);
		job.setMapperClass(DistributeCacheMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// 这里是我们正常的需要处理的数据所在路径
		FileInputFormat.setInputPaths(job, new Path("D:/tmp/mr/trade/trade_records.text"));
		FileOutputFormat.setOutputPath(job, new Path("D:/tmp/mr/trade/out_put_cache"));

		// 不需要reducer
		job.setNumReduceTasks(0);
		// 分发一个文件到task进程的工作目录
		job.addCacheFile(new URI("D:/tmp/mr/trade/cache.text"));

		// 分发一个归档文件到task进程的工作目录
//		job.addArchiveToClassPath(archive);

		// 分发jar包到task节点的classpath下
//		job.addFileToClassPath(jarfile);

		job.waitForCompletion(true);
	}

}
