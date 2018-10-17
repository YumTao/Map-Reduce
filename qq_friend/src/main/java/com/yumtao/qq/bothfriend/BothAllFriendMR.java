package com.yumtao.qq.bothfriend;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @src A-J O
 * @goal 对两两用户对共有的一个好友，统计成两两用户对共有的所有好友。
 * @action 比较简单，直接以key为两两用户对，对单个好友求和即可
 * @author yumTao
 *
 */
public class BothAllFriendMR {
	static class BothAllFriendMapper extends Mapper<LongWritable, Text, Text, Text> {

		/**
		 * @in A-J O
		 * @out A-J O mapp不做处理
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			try {
				sameInOut(value, context);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private void sameInOut(Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			System.out.println(String.format("once map oper: value=%s", line));
			if (StringUtils.isNotEmpty(line)) {
				String userCp = line.split("\t")[0];
				String friend = line.split("\t")[1];
				context.write(new Text(userCp), new Text(friend));
				System.out.println(String.format("once map write: key=%s, value=%s", userCp, friend));
			}
		}

	}

	static class BothAllFriendReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String userCp = key.toString();
			StringBuilder friendsCache = new StringBuilder();
			for (Text friend : values) {
				friendsCache.append(friend).append(",");
			}
			String bothAllfriends = friendsCache.subSequence(0, friendsCache.length() - 1).toString();
			context.write(new Text(userCp), new Text(bothAllfriends));
			System.out.println(String.format("once reduce write: key=%s, value=%s", userCp, bothAllfriends));
		}

	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");
		Job bothAllFriend = Job.getInstance(conf);
		bothAllFriend.setJarByClass(BothAllFriendMR.class);
		
		bothAllFriend.setMapperClass(BothAllFriendMapper.class);
		bothAllFriend.setReducerClass(BothAllFriendReducer.class);
		
		bothAllFriend.setMapOutputKeyClass(Text.class);
		bothAllFriend.setMapOutputValueClass(Text.class);
		bothAllFriend.setOutputKeyClass(Text.class);
		bothAllFriend.setOutputValueClass(Text.class);
		
		bothAllFriend.setCombinerClass(BothAllFriendReducer.class);
		
		FileInputFormat.setInputPaths(bothAllFriend, new Path("D:/tmp/mr/qq/out_bothone/part-r-00000"));
		FileOutputFormat.setOutputPath(bothAllFriend, new Path("D:/tmp/mr/qq/out_bothAll/"));
		
		boolean flag = bothAllFriend.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
		
	}
}
