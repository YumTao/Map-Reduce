package com.yumtao.flowcount;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * mapper: 分而治之任务中分业务，获取单个节点的结果集。
 * @author yumTao
 *
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		/**
		 * key:行数
		 * value:1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
		 */
		
		String line = value.toString();
		List<String> msgs = Arrays.asList(line.split("\t"));
		String phone = msgs.get(1);
		long upFlow = Long.valueOf(msgs.get(8));
		long downFlow =  Long.valueOf(msgs.get(9));
		long totalFlow = upFlow + downFlow;
		context.write(new Text(phone), new Text(upFlow + "," + downFlow + "," + totalFlow));
	}
	

}
