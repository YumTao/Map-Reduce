package com.yumtao.maxpricePerOrder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义GroupingComparator：
 * 步骤
 * 1.继承WritableComparator接口
 * 2.无参构造，传入对应Mapper的输出key
 * 3.重写compare(WritableComparable a, WritableComparable b)方法，来决定是否在同一组
 * 
 * @TODO 文件末尾追加Order_0000001	Pdt_02	1000.0 后，执行结果异常，如果将次记录放置在与其相同订单编号下又正常了。思考？？？
 * @author yumTao
 *
 */
public class OrderIdGroupingComparator extends WritableComparator {

	public OrderIdGroupingComparator() {
		super(OrderDetailVo.class, true);
	}

	/**
	 * 根据订单编号来决定是否在同一组
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderDetailVo left = (OrderDetailVo) a;
		OrderDetailVo right = (OrderDetailVo) b;
		return left.getOrderId().compareTo(right.getOrderId());
	}

}
