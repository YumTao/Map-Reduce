package com.yumtao.flowcount;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reducer： 对mapper的输出进行汇总统计，并输出
 * @author yumTao
 *
 */
public class FlowCountReducer extends Reducer<Text, Text, Text, Text> {

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
