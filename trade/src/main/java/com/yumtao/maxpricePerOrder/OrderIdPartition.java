package com.yumtao.maxpricePerOrder;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义partition，默认值是从0开始的
 * 
 * @author yumTao
 *
 */
public class OrderIdPartition extends Partitioner<OrderDetailVo, OrderDetailVo> {

	@Override
	public int getPartition(OrderDetailVo key, OrderDetailVo value, int numPartitions) {
		System.out.println(String.format("partition code : %s", key));
		return Integer.valueOf(key.getOrderId().substring(key.getOrderId().length() - 1, key.getOrderId().length()))
				- 1;
	}

}
