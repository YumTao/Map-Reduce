package com.yumtao.clickflow.step2;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yumtao.clickflow.step1.Step1;
import com.yumtao.clickflow.util.DateUtil;
import com.yumtao.clickflow.vo.AccessMsgByStep2;

/**
 * @author yumTao
 * @goal resources下，对step1的结果进行梳理，得到图step2格式
 * @mapper 构造所需要的格式{ session,cookie,timestamp,url,停留时长，第几步} 设置初始停留时长值 }
 * @notice1 自定义groupingComparator,使同一session的多条记录进入同一个reduce方法，根据不同记录的时间差来得到正确的页面停留时长。
 * @notice2 排序策略：先根据session正序，其次根据时间正序。
 * @reducer 计算停留时长，并输出。最后一个页面则输出默认停留时长
 */
public class Step2 {
	private static final Logger log = LoggerFactory.getLogger(Step2.class);

	static class Step2Mapper extends Mapper<LongWritable, Text, AccessMsgByStep2, AccessMsgByStep2> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, AccessMsgByStep2, AccessMsgByStep2>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] split = line.split("\t");
			String timestamp = split[0];
			String ip = split[1];
			String cookie = split[2];
			String session = split[3];
			String url = split[4];
			String referal = split[5];
			AccessMsgByStep2 msg = new AccessMsgByStep2(timestamp, ip, cookie, session, url, referal);
			context.write(msg, msg);
			log.debug("map write value= {}", msg.toString());
		}

	}

	static class Step2Reducer extends Reducer<AccessMsgByStep2, AccessMsgByStep2, AccessMsgByStep2, NullWritable> {

		private final static long DEFAULT_STAYTIME = 30;

		@Override
		protected void reduce(AccessMsgByStep2 key, Iterable<AccessMsgByStep2> msgs,
				Reducer<AccessMsgByStep2, AccessMsgByStep2, AccessMsgByStep2, NullWritable>.Context context)
				throws IOException, InterruptedException {

			int step = 1;
			AccessMsgByStep2 old = null;
			for (AccessMsgByStep2 iter : msgs) {
				AccessMsgByStep2 now = cloneNew(iter);
				now.setStep(step++);
				now.setStayTime(DEFAULT_STAYTIME); // 设置默认停留时间
				if (null != old) {
					// 更正上一个停留时间（new - old）
					old.setStayTime(DateUtil.getSecBetween(old.getTime(), now.getTime()));
					// 写出上一个
					context.write(old, NullWritable.get());
					log.debug("reduce write {}", old.toString());
				}
				old = now;
			}
			context.write(old, NullWritable.get());
			log.warn("reduce write {}", old.toString());
		}

		private AccessMsgByStep2 cloneNew(AccessMsgByStep2 old) {
			return new AccessMsgByStep2(old.getTimestamp(), old.getIp(), old.getCookie(), old.getSession(),
					old.getUrl(), old.getReferal());
		}

	}

	public static void main(String[] args) throws Exception {
		deleteDir("D:/tmp/mr/click_flow/step2");

		Configuration conf = new Configuration();
		Job step2 = Job.getInstance(conf);
		conf.set("mapreduce.framework.name", "local");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");

		step2.setJarByClass(Step1.class);

		step2.setGroupingComparatorClass(SessionGroup.class);

		step2.setMapperClass(Step2Mapper.class);
		step2.setReducerClass(Step2Reducer.class);

		step2.setMapOutputKeyClass(AccessMsgByStep2.class);
		step2.setMapOutputValueClass(AccessMsgByStep2.class);
		step2.setOutputKeyClass(AccessMsgByStep2.class);
		step2.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(step2, new Path("D:/tmp/mr/click_flow/step1/part-r-00000"));
		FileOutputFormat.setOutputPath(step2, new Path("D:/tmp/mr/click_flow/step2"));

		boolean flag = step2.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}

	private static void deleteDir(String dirpath) throws IOException {

		Executors.newSingleThreadExecutor().execute(() -> {
			try {
				File descDir = new File(dirpath);
				FileUtils.forceDeleteOnExit(descDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});

	}

}