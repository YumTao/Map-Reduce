package com.yumtao.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * mr会将相同的key放置在同一组 生命周期：框架每传递进来一个kv 组，reduce方法被调用一次
 * 
 * @author 56243
 *
 */
public class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		System.out.println(String.format("once combine oper: key=%s", key.toString()));
		// 传进来的都是相同key的一组，如 hello, [1,1,3,4]
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}

}
