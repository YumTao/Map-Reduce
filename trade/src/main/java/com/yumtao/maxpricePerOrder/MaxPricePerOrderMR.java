package com.yumtao.maxpricePerOrder;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @goal trade_records.txt文件求每笔订单中最大交易额的记录
 * @action1 自定义partition，根据订单编号进行分区，并根据价格进行排序。得到按订单编号个数量的文件，且是根据价格倒序。取每个文件的第一行数据即可。
 *          弊端：订单编号越多，所对应的reduce个数也越多，reduce输出文件也越多，显然是不合理的。
 * 
 * @action2 自定义reduce的kv分组策略（GroupingComparator），让同一个订单编号的进入同一个reduce（）方法，然后再其中查找价格最高的一条记录进行输出。
 * 
 * @author yumTao
 *
 */
public class MaxPricePerOrderMR {
	private static final Logger log = LoggerFactory.getLogger(MaxPricePerOrderMR.class);

	static class MaxPricePerOrderMapper extends Mapper<LongWritable, Text, OrderDetailVo, OrderDetailVo> {

		// TODO 分布式缓存集群测试
		@Override
		protected void setup(Mapper<LongWritable, Text, OrderDetailVo, OrderDetailVo>.Context context)
				throws IOException, InterruptedException {
			URI[] cacheFiles = context.getCacheFiles();
			if (null != cacheFiles && cacheFiles.length > 0) {
				for (URI uri : cacheFiles) {
					log.error(uri.getPath());
				}
			}
		}

		/**
		 * 构造OrderDetailVo，OrderDetailVo输出
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, OrderDetailVo, OrderDetailVo>.Context context)
				throws IOException, InterruptedException {
			resovleMap(value, context);
		}

		private void resovleMap(Text value, Mapper<LongWritable, Text, OrderDetailVo, OrderDetailVo>.Context context)
				throws IOException, InterruptedException {
			try {
				String line = value.toString();
				log.debug("once mapper oper value={}", line);
				String orderId = line.split("\t")[0];
				String productId = line.split("\t")[1];
				double price = Double.valueOf(line.split("\t")[2]);

				OrderDetailVo vo = new OrderDetailVo(orderId, productId, price);
				context.write(vo, vo);
				log.debug("once mapper write key={} value={}", vo.toString(), vo);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}

	}

	static class MaxPricePerOrderReduce extends Reducer<OrderDetailVo, OrderDetailVo, Text, Text> {

		/**
		 * 同一订单编号集合中，获取价格最高的vo，转换成文本后输出
		 */
		@Override
		protected void reduce(OrderDetailVo key, Iterable<OrderDetailVo> value,
				Reducer<OrderDetailVo, OrderDetailVo, Text, Text>.Context context)
				throws IOException, InterruptedException {

			List<OrderDetailVo> records = new ArrayList<>();
			for (OrderDetailVo vo : value) {
				records.add(new OrderDetailVo(vo.getOrderId(), vo.getProductId(), vo.getPrice()));
			}
			log.debug("once reduce oper key=%s, value=%s", key.toString(), records);

			OrderDetailVo maxPriceVo = records.stream().reduce((vo, anotherVo) -> {
				return vo.getPrice() > anotherVo.getPrice() ? vo : anotherVo;
			}).get();

			log.debug("max price vo is {}", maxPriceVo.toString());
			log.debug("once reduce write key={}, value={}", "max price records", maxPriceVo.toString());
			context.write(new Text("max price records"), new Text(maxPriceVo.toString()));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.set("mapreduce.framework.name", "local");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "singlenode");
		Job maxpriceJob = Job.getInstance(conf);

		maxpriceJob.setJarByClass(MaxPricePerOrderMR.class);
		maxpriceJob.setMapperClass(MaxPricePerOrderMapper.class);
		maxpriceJob.setReducerClass(MaxPricePerOrderReduce.class);

		maxpriceJob.setMapOutputKeyClass(OrderDetailVo.class);
		maxpriceJob.setMapOutputValueClass(OrderDetailVo.class);
		maxpriceJob.setOutputKeyClass(Text.class);
		maxpriceJob.setOutputValueClass(Text.class);

		maxpriceJob.setGroupingComparatorClass(OrderIdGroupingComparator.class);
		maxpriceJob.setPartitionerClass(OrderIdPartition.class);
		maxpriceJob.setNumReduceTasks(3);

//		FileInputFormat.setInputPaths(maxpriceJob, new Path("D:/tmp/mr/trade"));
//		FileOutputFormat.setOutputPath(maxpriceJob, new Path("D:/tmp/mr/trade/out_maxprice"));
		FileInputFormat.setInputPaths(maxpriceJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(maxpriceJob, new Path(args[1]));

		boolean flag = maxpriceJob.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}
}
