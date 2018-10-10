package com.yumtao.flowcount.mypartition.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowAndPhoneVo implements WritableComparable<FlowAndPhoneVo> {
	private long upFlow;
	private long downFlow;
	private long totalFlow;
	private String phone;

	/**
	 * notice:序列化方法必须与反序列换方法顺序一致
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(totalFlow);
		out.writeUTF(phone);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		totalFlow = in.readLong();
		phone = in.readUTF();
	}

	/**
	 * order desc
	 */
	@Override
	public int compareTo(FlowAndPhoneVo o) {
		return this.totalFlow > o.getTotalFlow() ? -1 : 1;
	}

	/**
	 * notice必须给出无参构造方法
	 */
	public FlowAndPhoneVo() {
	}

	public FlowAndPhoneVo(long upFlow, long downFlow, long totalFlow, String phone) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.totalFlow = totalFlow;
		this.phone = phone;
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

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	@Override
	public String toString() {
		return "FlowAndPhoneVo [upFlow=" + upFlow + ", downFlow=" + downFlow + ", totalFlow=" + totalFlow + ", phone="
				+ phone + "]";
	}

}
