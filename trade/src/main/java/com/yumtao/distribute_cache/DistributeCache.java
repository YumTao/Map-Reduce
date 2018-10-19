package com.yumtao.distribute_cache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributeCache extends Mapper<LongWritable, Text, Text, Text> {

	FileReader in = null;
	BufferedReader reader = null;
	Map<String, String> b_tab = new HashMap<String, String>();
	String localpath = null;
	String uirpath = null;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// 通过这几句代码可以获取到cache file的本地绝对路径，测试验证用
		URI[] cacheFiles = context.getCacheFiles();

		// 缓存文件的用法——直接用本地IO来读取
		// 这里读的数据是map task所在机器本地工作目录中的一个小文件
		in = new FileReader("b.txt");
		reader = new BufferedReader(in);
		String line = null;
		while (null != (line = reader.readLine())) {

			String[] fields = line.split(",");
			b_tab.put(fields[0], fields[1]);

		}
		IOUtils.closeStream(reader);
		IOUtils.closeStream(in);
	}

}
