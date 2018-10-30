package com.yumtao.clickflow.step1;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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

import com.yumtao.clickflow.util.DateUtil;
import com.yumtao.clickflow.vo.AccessMsg;

/**
 * @author yumTao
 * @mapper: 构造浏览记录对象{ 时间戳，ip,cookie,session,url,referal格式，设置初始session值 }， 根据时间正序排列
 * @reducer： 比对两条同一ip的浏览记录对象，判断是否时间差在30分钟内，进而决定是否是同一个session
 *
 */
public class Step1 {

	private static final Logger log = LoggerFactory.getLogger(Step1.class);

	static class Step1Mapper extends Mapper<LongWritable, Text, AccessMsg, NullWritable> {

		String session = "session_";

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, AccessMsg, NullWritable>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String ip = "null";
			String timestamp = "null";
			String url = "null";
			String referal = "null";
			try {
				ip = line.split(" ")[0];
				timestamp = line.split(" ")[3];
				timestamp = DateUtil.format(DateUtil.parse(timestamp.substring(1, timestamp.length()), DateUtil.srcSdf),
						DateUtil.destSdf);
				
				int firstQuotes = line.indexOf("\"");
				int secondeQuotes = line.indexOf("\"", firstQuotes + 1);
				int thirdQuotes = line.indexOf("\"", secondeQuotes + 1);
				int fourthQuotes = line.indexOf("\"", thirdQuotes + 1);
				String serverPath = line.substring(firstQuotes + 1, secondeQuotes);
				referal = line.substring(thirdQuotes + 1, fourthQuotes);
				url = serverPath.split(" ")[1];
			} catch (Exception e) {
			}

			long sessionNum = context.getCounter("counter", "session").getValue();
			AccessMsg msg = new AccessMsg(timestamp, ip, "defalutcookie", session + sessionNum, url, referal);
			context.getCounter("counter", "session").increment(1);
			context.write(msg, NullWritable.get());
			log.debug("mapper write {}", msg.toString());
		}

	}

	static class Step1Reducer extends Reducer<AccessMsg, NullWritable, AccessMsg, NullWritable> {

		private static Map<String, String> ip2DateSession = new HashMap<>(); // ip=日期&session缓存

		private final String DATE_SESSION_SPLIT = "&";
		private final long SESSION_TIMEOUT = 30;

		@Override
		protected void reduce(AccessMsg key, Iterable<NullWritable> value,
				Reducer<AccessMsg, NullWritable, AccessMsg, NullWritable>.Context context)
				throws IOException, InterruptedException {

			log.debug("reducer read {}", key.toString());
			if (null != ip2DateSession.get(key.getIp())) {

				// 同一ip的两次访问，访问时间差在30分钟内的，认定为同一个session,将时间更近的session设置成时间更远的session
				String datestr = ip2DateSession.get(key.getIp()).split(DATE_SESSION_SPLIT)[0];
				String session = ip2DateSession.get(key.getIp()).split(DATE_SESSION_SPLIT)[1];
				Date agoDate = DateUtil.parse(datestr, DateUtil.destSdf);
				long millMinBetween = DateUtil.getMillMinBetween(agoDate, key.getTime());
				if (millMinBetween < SESSION_TIMEOUT) {
					key.setSession(session);
				}
			}
			// 缓存中放入该ip时间最近的日期&session
			ip2DateSession.put(key.getIp(), key.getTimestamp() + DATE_SESSION_SPLIT + key.getSession());

			context.write(key, NullWritable.get());
			log.debug("reducer write {}", key.toString());

		}

	}

	public static void main(String[] args) throws Exception {

		File descDir = new File("D:/tmp/mr/click_flow/step1");
		FileUtils.forceDeleteOnExit(descDir);
		
		Configuration conf = new Configuration();
		Job step1 = Job.getInstance(conf);
		conf.set("mapreduce.framework.name", "local");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");

		step1.setJarByClass(Step1.class);

		step1.setMapperClass(Step1Mapper.class);
		step1.setReducerClass(Step1Reducer.class);

		step1.setMapOutputKeyClass(AccessMsg.class);
		step1.setMapOutputValueClass(NullWritable.class);
		step1.setOutputKeyClass(AccessMsg.class);
		step1.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(step1, new Path("D:/tmp/mr/click_flow/access.log.fensi"));
		FileOutputFormat.setOutputPath(step1, new Path("D:/tmp/mr/click_flow/step1"));

		boolean flag = step1.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}

}
