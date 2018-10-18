package com.yumtao.maxpricePerOrder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderDetailVo implements WritableComparable<OrderDetailVo> {

	private String orderId;
	private String productId;
	private double price;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orderId);
		out.writeUTF(productId);
		out.writeDouble(price);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.orderId = in.readUTF();
		this.productId = in.readUTF();
		this.price = in.readDouble();
	}

	@Override
	public int compareTo(OrderDetailVo orderDetailVo) {
		return 1;
//		return this.price > orderDetailVo.getPrice() ? -1 : 1;
//		return this.orderId.compareTo(orderDetailVo.getOrderId());
	}

	public OrderDetailVo() {
	}

	public OrderDetailVo(String orderId, String productId, double price) {
		this.orderId = orderId;
		this.productId = productId;
		this.price = price;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	@Override
	public String toString() {
		return "OrderDetailVo [orderId=" + orderId + ", productId=" + productId + ", price=" + price + "]";
	}

}
