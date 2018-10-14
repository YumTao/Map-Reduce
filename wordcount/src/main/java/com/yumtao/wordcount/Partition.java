package com.yumtao.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		System.out.println(String.format("once partition oper: key=%s", key.toString()));
		return key.toString().equals("i") ? 0 : 1;
	}

}
