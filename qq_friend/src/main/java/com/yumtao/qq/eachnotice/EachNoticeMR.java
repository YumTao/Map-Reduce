package com.yumtao.qq.eachnotice;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.yumtao.qq.bothfriend.BothOneFriendMR;
import com.yumtao.qq.util.MyStringUtil;

/**
 * @src A:B,C,D,F,E,O B:A,C,E,K
 * @goal 获取互相关注的用户组
 * @action map阶段拆分出一对一的关系，key为两用户关系，value为关注次数。如 A-B 1
 *         上诉的key按字典顺序对字母进行排序，从而使A-B，B-A都为A-B。 reduce阶段，统计关注次数，对关注次数为2的进行输出。
 * 
 * @author yumTao
 *
 */
public class EachNoticeMR {

	static class EachNoticeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			System.out.println(String.format("once map oper: value=%s", line));
			if (StringUtils.isNotEmpty(line)) {
				try {
					writeOne2OneNotice(context, line);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		private void writeOne2OneNotice(Mapper<LongWritable, Text, Text, IntWritable>.Context context, String line) {
			String user = line.split(":")[0];
			String friends = line.split(":")[1];
			Arrays.asList(friends.split(",")).stream().forEach(friend -> {
				String user2Friend = user + "-" + friend;
				String seqUserCp = MyStringUtil.seqUserCp(user2Friend, "-");
				try {
					context.write(new Text(seqUserCp), new IntWritable(1));
					System.out.println(String.format("once map write: key=%s, value=%s", seqUserCp, 1));
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}

	}

	static class EachNoticeReducer extends Reducer<Text, IntWritable, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			String user2user = key.toString();
			int count = 0;
			for (IntWritable noticeCount : values) {
				count += noticeCount.get();
			}
			System.out.println(String.format("once reduce oper: key=%s, noticeCount=%d", key, count));
			if (count > 0 && count % 2 == 0) {
				String user = user2user.split("-")[0];
				String anotherUser = user2user.split("-")[1];
				context.write(new Text(user), new Text(anotherUser));
				System.out.println(String.format("once reduce write: user=%s, anotherUser=%s", user, anotherUser));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");
		Job eachNoticeJob = Job.getInstance(conf);
		eachNoticeJob.setJarByClass(BothOneFriendMR.class);

		eachNoticeJob.setMapperClass(EachNoticeMapper.class);
		eachNoticeJob.setReducerClass(EachNoticeReducer.class);

		eachNoticeJob.setMapOutputKeyClass(Text.class);
		eachNoticeJob.setMapOutputValueClass(IntWritable.class);

		eachNoticeJob.setOutputKeyClass(Text.class);
		eachNoticeJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(eachNoticeJob, new Path("D:/tmp/mr/qq/data.txt"));
		FileOutputFormat.setOutputPath(eachNoticeJob, new Path("D:/tmp/mr/qq/each_notice"));

		boolean flag = eachNoticeJob.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}

}
