package com.yumtao.clickflow.step2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.yumtao.clickflow.vo.AccessMsgByStep;

public class SessionGroup extends WritableComparator {
	public SessionGroup() {
		super(AccessMsgByStep.class, true);
	}

	/**
	 * 根据session来决定是否在同一组
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		AccessMsgByStep left = (AccessMsgByStep) a;
		AccessMsgByStep right = (AccessMsgByStep) b;
		System.out.println(String.format("grouping compare left=%s, right=%s", left.toString(), right.toString()));
		return left.getSession().compareTo(right.getSession());
	}
}
