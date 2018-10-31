package com.yumtao.clickflow.step3;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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
import com.yumtao.clickflow.step2.SessionGroup;
import com.yumtao.clickflow.vo.AccessMsgByStep2;
import com.yumtao.clickflow.vo.AccessMsgStep3Out;

/**
 * @author yumTao
 * @goal resources下，对step1的结果进行梳理，得到图step3格式
 * @mapper 构造所需要的格式{ session,cookie,timestamp,url,停留时长，第几步} 设置初始停留时长值 }
 * @notice1 自定义groupingComparator,使同一session的多条记录进入同一个reduce方法，根据不同记录来构造该用户的浏览起点与终点。
 * @notice2 排序策略：先根据session正序，其次根据时间正序。
 * @reducer 得到该用户的访问起始时间、结束时间、进入页面、结束页面等浏览痕迹，构造输出对象，并输出
 */
public class Step3 {
	private static final Logger log = LoggerFactory.getLogger(Step3.class);

	static class Step3Mapper extends Mapper<LongWritable, Text, AccessMsgByStep2, AccessMsgByStep2> {

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

	static class Step3Reducer extends Reducer<AccessMsgByStep2, AccessMsgByStep2, AccessMsgStep3Out, NullWritable> {

		private AccessMsgByStep2 endMsg;

		@Override
		protected void reduce(AccessMsgByStep2 key, Iterable<AccessMsgByStep2> msgs,
				Reducer<AccessMsgByStep2, AccessMsgByStep2, AccessMsgStep3Out, NullWritable>.Context context)
				throws IOException, InterruptedException {

			AccessMsgStep3Out result = new AccessMsgStep3Out();
			int step = 1;
			Set<String> pageCache = new HashSet<>();
			for (AccessMsgByStep2 tmp : msgs) {
				if (step++ == 1) {
					result.setSession(tmp.getSession());
					result.setIp(tmp.getIp());
					result.setCookie(tmp.getCookie());
					result.setStarttime(tmp.getTimestamp());
					result.setStarturl(tmp.getUrl());
					result.setReferal(tmp.getReferal());
				}
				pageCache.add(tmp.getUrl());
				endMsg = tmp;
			}

			result.setEndtime(endMsg.getTimestamp());
			result.setEndurl(endMsg.getUrl());
			result.setAccessPageNum(pageCache.size());

			context.write(result, NullWritable.get());
			log.warn("reduce write {}", result.toString());
		}

	}

	public static void main(String[] args) throws Exception {
		deleteDir("D:/tmp/mr/click_flow/step3");

		Configuration conf = new Configuration();
		Job step2 = Job.getInstance(conf);
		conf.set("mapreduce.framework.name", "local");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");

		step2.setJarByClass(Step1.class);

		step2.setGroupingComparatorClass(SessionGroup.class);

		step2.setMapperClass(Step3Mapper.class);
		step2.setReducerClass(Step3Reducer.class);

		step2.setMapOutputKeyClass(AccessMsgByStep2.class);
		step2.setMapOutputValueClass(AccessMsgByStep2.class);
		step2.setOutputKeyClass(AccessMsgStep3Out.class);
		step2.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(step2, new Path("D:/tmp/mr/click_flow/step1/part-r-00000"));
		FileOutputFormat.setOutputPath(step2, new Path("D:/tmp/mr/click_flow/step3"));

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