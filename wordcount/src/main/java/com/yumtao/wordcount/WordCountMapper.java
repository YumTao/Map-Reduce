package com.yumtao.wordcount;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 统计单词出现个数 map&reduce: 分而治之思想， map负责多工位毛加工，reduce负责统一处理至成品
 * 
 * @author yumTao
 *
 */
// Mapper<LongWritable, Text, Text, IntWritable> keyInput, valueInput, keyOutput, valueOutput
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// key 为每次读取的行数
		// value 为每次读取的文本值

		// 1.切分文本，到 单词=次数 map
		Map<String, Integer> word2Count = new HashMap<>();
		String line = value.toString();
		Arrays.asList(line.split(" ")).stream().forEach(word -> {
			if (StringUtils.isNotEmpty(word.trim())) {
				int count = word2Count.get(word) == null ? 0 : word2Count.get(word);
				word2Count.put(word, ++count);
			}
		});

		// 2.输出
		word2Count.forEach((word, count) -> {
			try {
				context.write(new Text(word), new IntWritable(count));
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}

}
