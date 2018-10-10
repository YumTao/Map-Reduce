package com.yumtao.flowcount.mypartition;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.yumtao.flowcount.mypartition.vo.FlowAndPhoneVo;

public class PhonePartition extends Partitioner<FlowAndPhoneVo, Text> {

	static Map<String, Integer> provinceMap = new HashMap<String, Integer>();

	static {
		provinceMap.put("135", 0);
		provinceMap.put("136", 1);
		provinceMap.put("137", 2);
		provinceMap.put("138", 3);
		provinceMap.put("139", 4);
	}

	@Override
	public int getPartition(FlowAndPhoneVo key, Text value, int numPartitions) {
		String phoneHead = key.getPhone().substring(0, 3);
		Integer code = provinceMap.get(phoneHead);
		return code == null ? 5 : code.intValue();
	}

}
