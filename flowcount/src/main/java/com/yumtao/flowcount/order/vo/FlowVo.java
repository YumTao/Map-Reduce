package com.yumtao.flowcount.order.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowVo implements WritableComparable<FlowVo> {

	private long upFlow;
	private long downFlow;
	private long totalFlow;

	/**
	 * notice:序列化方法必须与反序列换方法顺序一致
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(totalFlow);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		totalFlow = in.readLong();
	}

	/**
	 * order desc
	 */
	@Override
	public int compareTo(FlowVo o) {
		return this.totalFlow > o.getTotalFlow() ? -1 : 1;
	}

	/**
	 * notice必须给出无参构造方法
	 */
	public FlowVo() {
	}

	public FlowVo(long upFlow, long downFlow, long totalFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.totalFlow = totalFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getTotalFlow() {
		return totalFlow;
	}

	public void setTotalFlow(long totalFlow) {
		this.totalFlow = totalFlow;
	}

}
