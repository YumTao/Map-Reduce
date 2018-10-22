package com.yumtao.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobController {
	private static final Logger log = LoggerFactory.getLogger(MRCounter.class);

	private static final String BOTH_ALL_OUTPUT_FOLDER = "D:/tmp/mr/qq/out_bothAll/";
	private static final String BOTH_ONE_OUTPUT_FOLDER = "D:/tmp/mr/qq/out_bothone/";
	private static final String INPUT_PATH = "D:/tmp/mr/qq/data.txt";

	static class BothOneFriendMapper extends Mapper<LongWritable, Text, Text, Text> {

		/**
		 * @in: A:B,C,D,F,E,O
		 * @out: <B,A><C,A><D,A><F,A><E,A><O,A>
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// A:B,C,D,F,E,O
			try {
				writeFriend2User(value, context);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			log.warn("both one job mapper start");
		}

		private void writeFriend2User(Text value, Mapper<LongWritable, Text, Text, Text>.Context context) {
			log.debug("once map oper: value={}", value.toString());
			String user2Friend = value.toString();
			String user = user2Friend.split(":")[0];
			String friends = user2Friend.split(":")[1];
			Arrays.asList(friends.split(",")).stream().forEach(friend -> {
				try {
					context.write(new Text(friend), new Text(user));
					log.debug("once map write: key={}, value={}", friend, user);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}

	}

	static class BothOneFriendReducer extends Reducer<Text, Text, Text, Text> {

		/**
		 * @in: <C,[A,B]>
		 * @out: A-B:C
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String friend = key.toString();
			List<String> users = new ArrayList<>(); // 用户组
			for (Text text : values) {
				users.add(text.toString());
			}
			log.debug("once reduce oper: key={}, values={}", key, users);

			// 用户组：[A,B,C] -> A-B,A-C,B-C 梯形，遍历嵌套
			for (int i = 0; i < users.size(); i++) {
				for (int j = i + 1; j < users.size(); j++) {
					String userCp = users.get(i) + "-" + users.get(j);
					String seqUserCp = MyStringUtil.seqUserCp(userCp, "-");
					context.write(new Text(seqUserCp), new Text(friend));
					log.debug("once reduce write: key={}, value={}", seqUserCp, friend);
				}
			}
		}

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			log.warn("both one job reducer start");
		}

	}

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

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			log.warn("both ALL job mapper start");
		}

		private void sameInOut(Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			log.debug("once map oper: value={}", line);
			if (StringUtils.isNotEmpty(line)) {
				String userCp = line.split("\t")[0];
				String friend = line.split("\t")[1];
				context.write(new Text(userCp), new Text(friend));
				log.debug("once map write: key={}, value={}", userCp, friend);
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
			log.debug("once reduce write: key={}, value={}", userCp, bothAllfriends);
		}

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			log.warn("both ALL job reducer start");
		}

	}

	public static void main(String[] args) throws Exception {

		FileUtils.forceDelete(new File(BOTH_ALL_OUTPUT_FOLDER));
		FileUtils.forceDelete(new File(BOTH_ONE_OUTPUT_FOLDER));

		Job bothOneJob = getBothOneJob();
		Job bothAllJob = getBothAllJob();

		ControlledJob cBothOneJob = new ControlledJob(bothOneJob.getConfiguration());
		ControlledJob cBothAllJob = new ControlledJob(bothAllJob.getConfiguration());

		// set dependency 设置作业依赖关系:求两两用户对所有共同好友的job依赖与两两用户对单个好友
		cBothAllJob.addDependingJob(cBothOneJob);

		JobControl jobControl = new JobControl("getCpUsersCommonFriends");
		jobControl.addJob(cBothOneJob);
		jobControl.addJob(cBothAllJob);

		cBothOneJob.setJob(bothOneJob);
		cBothAllJob.setJob(bothAllJob);

		// 新建一个线程来运行已加入JobControl中的作业，开始进程并等待结束
		Thread thread = new Thread(jobControl);
		thread.start();
		while (!jobControl.allFinished()) {
			Thread.sleep(500);
		}
		jobControl.stop();

		System.exit(0);
	}

	private static Job getBothAllJob() throws IOException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		Job bothAllFriend = Job.getInstance(conf);
		bothAllFriend.setJarByClass(JobController.class);

		bothAllFriend.setMapperClass(BothAllFriendMapper.class);
		bothAllFriend.setReducerClass(BothAllFriendReducer.class);

		bothAllFriend.setMapOutputKeyClass(Text.class);
		bothAllFriend.setMapOutputValueClass(Text.class);
		bothAllFriend.setOutputKeyClass(Text.class);
		bothAllFriend.setOutputValueClass(Text.class);

		bothAllFriend.setCombinerClass(BothAllFriendReducer.class);

		FileInputFormat.setInputPaths(bothAllFriend, new Path(BOTH_ONE_OUTPUT_FOLDER + "/part-r-00000"));
		FileOutputFormat.setOutputPath(bothAllFriend, new Path(BOTH_ALL_OUTPUT_FOLDER));
		return bothAllFriend;
	}

	private static Job getBothOneJob() throws IOException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		Job bothOneFriendJob = Job.getInstance(conf);
		bothOneFriendJob.setJarByClass(JobController.class);

		bothOneFriendJob.setMapperClass(BothOneFriendMapper.class);
		bothOneFriendJob.setReducerClass(BothOneFriendReducer.class);

		bothOneFriendJob.setMapOutputKeyClass(Text.class);
		bothOneFriendJob.setMapOutputValueClass(Text.class);

		bothOneFriendJob.setOutputKeyClass(Text.class);
		bothOneFriendJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(bothOneFriendJob, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(bothOneFriendJob, new Path(BOTH_ONE_OUTPUT_FOLDER));

		return bothOneFriendJob;
	}
	
}
