package com.yumtao.clickflow.step2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yumtao.clickflow.vo.AccessMsgByStep2;

public class SessionGroup extends WritableComparator {
	private static final Logger log = LoggerFactory.getLogger(SessionGroup.class);
	
	public SessionGroup() {
		super(AccessMsgByStep2.class, true);
	}

	/**
	 * 根据session来决定是否在同一组
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		AccessMsgByStep2 left = (AccessMsgByStep2) a;
		AccessMsgByStep2 right = (AccessMsgByStep2) b;
		log.debug("grouping compare left={}, right={}", left.toString(), right.toString());
		return left.getSession().compareTo(right.getSession());
	}
}
