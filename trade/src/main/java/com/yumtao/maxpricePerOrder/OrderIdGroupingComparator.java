package com.yumtao.maxpricePerOrder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义GroupingComparator： 步骤 1.继承WritableComparator接口 2.无参构造，传入对应Mapper的输出key
 * 3.重写compare(WritableComparable a, WritableComparable b)方法，来决定是否在同一组
 * 
 * @question 文件末尾追加Order_0000001 Pdt_02 1000.0
 *           后，执行结果异常，如果将次记录放置在与其相同订单编号下又正常了。思考？？？
 * @answer reduce执行<k,v>分组时，是对所在分区，分别读取两次数据，对当前两次数据进行比对，如在同一组则继续向下取一个，并判断是否在同一组，直至获取到不同组的为止，进而执行reduce（）方法业务
 *         而默认的读取方法是按行读取。 
 *         
 *         eg : {1.1,2.1,3.2,4.1} 格式:(index.group)
 *          -> (1.1,2.1):同一组，继续
 *          -> (2.1,3.2)：不同组，将获取的同组结果[1.1,2.1]传给reduce（）执行具体业务。
 *          -> (3.2, 4.1)不同组，将获取的同组结果[3.2]传给reduce（）执行具体业务。
 *          -> 只剩下最后一个4.1，此时无需执行分组，默认自成一组传给reduce（）执行具体业务。
 * 
 * @notice 要出现在reduce<k,v>同一组中，如果不进行分区的话，那么预期同一组的数据在行数上必须连续
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
		System.out.println(String.format("grouping compare left=%s, right=%s", left.toString(), right.toString()));
		return left.getOrderId().compareTo(right.getOrderId());
	}

}
