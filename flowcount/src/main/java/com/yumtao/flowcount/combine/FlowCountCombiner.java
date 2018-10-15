package com.yumtao.flowcount.combine;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.yumtao.flowcount.combine.vo.FlowAndPhoneVoForCombine;

/**
 * @desc combine的输入要与mapper输出相对应，combine的输出要与reduce的输入相对应
 * @author yumTao
 *
 */
public class FlowCountCombiner extends Reducer<FlowAndPhoneVoForCombine, Text, FlowAndPhoneVoForCombine, Text> {

	@Override
	protected void reduce(FlowAndPhoneVoForCombine key, Iterable<Text> value,
			Reducer<FlowAndPhoneVoForCombine, Text, FlowAndPhoneVoForCombine, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println(String.format("once combine oper:key=%s", key.toString()));
		for (Text text : value) {
			context.write(key, text);
		}

	}

}