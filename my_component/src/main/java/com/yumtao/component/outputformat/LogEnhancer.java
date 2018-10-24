package com.yumtao.component.outputformat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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

/**
 * @goal 这个程序是根据url查询规则知识库，进而输出url及对应内容到文件，查询不到内容的输出到待爬取文件中
 * 
 * @author
 * 
 */
public class LogEnhancer {

	static class LogEnhancerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		Map<String, String> knowledgeMap = new HashMap<String, String>();

		/**
		 * maptask在初始化时会先调用setup方法一次 利用这个机制，将外部的知识库加载到maptask执行的机器内存中
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			DBLoader.dbLoader(knowledgeMap);

		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			String[] fields = StringUtils.split(line, "\t");

			try {
				String url = fields[1];

				// 对这一行日志中的url去知识库中查找内容分析信息
				String content = knowledgeMap.get(url);

				// 根据内容信息匹配的结果，来构造两种输出结果
				String result = "";
				if (null == content) {
					// 输往待爬清单的内容
					result = url + "\t" + "tocrawl\n";
				} else {
					// 输往增强日志的内容
					result = line + "\t" + content + "\n";
				}

				context.write(new Text(result), NullWritable.get());
			} catch (Exception e) {

			}
		}

	}

	static class MyReduce extends Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}

	}

	public static void main(String[] args) throws Exception {
		args = new String[2];
		args[0] = "D:/tmp/mr/urlcontent/url.txt";
		args[1] = "D:/tmp/mr/urlcontent/out";

		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf);

		job.setJarByClass(LogEnhancer.class);

		job.setMapperClass(LogEnhancerMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// reduce不做任务业务，所以使用默认的reduce即可
		job.setReducerClass(MyReduce.class);

		// 要将自定义的输出格式组件设置到job中
//		job.setOutputFormatClass(LogEnhancerOutputFormat.class);
		job.setOutputFormatClass(MyOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
		// 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean flag = job.waitForCompletion(true);
		System.exit(flag ? 0 : 1);

	}

}
