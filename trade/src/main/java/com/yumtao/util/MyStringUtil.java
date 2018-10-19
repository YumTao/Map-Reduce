package com.yumtao.util;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

public class MyStringUtil {
	/**
	 * @goal 对用户对进行排序，如 A-B -> A-B, B-A -> A-B 为后续操作带来便利
	 * 
	 * @param srcStr 原数据
	 * @param splitStr 原数据分隔符
	 * @return
	 */
	public static String seqUserCp(String srcStr, String splitStr) {
		Set<String> userSet = new TreeSet<>();
		userSet.addAll(Arrays.asList(srcStr.split(splitStr)));

		StringBuilder seqUserCpCache = new StringBuilder();
		userSet.stream().forEach(user -> seqUserCpCache.append(user).append(splitStr));
		return seqUserCpCache.subSequence(0, seqUserCpCache.length() - 1).toString();
	}
}
