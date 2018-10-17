package com.yumtao.qq.bothfriend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
 * @src 原数据格式 A:B,C,D,F,E,O B:A,C,E,K
 * 
 * @goal:对data.txt的文件中的内容进行，求出两两之间有共同好友的"用户对"，及他俩的共同好友,比如:a-b : c ,e。
 * @action1 常规思路：A:B,C,D,F,E,O 与 B:A,C,E,K 取交集，不考虑使用分布式缓存时，在map阶段是无法完成交集业务的。
 *          如果在reduce中取交集，那么必须要让所有的数据在同一个key中，那么文件过大时，会造成内存溢出的问题。所以常规思路不可取。
 * 
 * @action2 反向思路：结果是求用户对的共同好友，那么如果以好友为key，好友所属的用户为value，就可以得到当前好友key的所有所属用户，也就是这些用户有共同的好友key。
 *          <好友，所属用户> 第一行：<B,A><C,A><D,A><F,A><E,A><O,A>
 *          第二行：<A,B><C,B><E,B><K,B> reduce阶段：<C,[A,B]>
 *          再将这些用户按两两输出，最后能得到的结果是，两两用户对所共有的好友，但是这里共有的好友每一行数据只有一个值。 <A-B,C>
 * 
 *          再次运行一个MR程序，对两两用户对的共同好友进行求和，就完成的goal需求
 * @author yumTao
 * @desc 当前为获取两两用户的单个共同好友的MR程序
 *
 */
public class BothOneFriendMR {

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

		private void writeFriend2User(Text value, Mapper<LongWritable, Text, Text, Text>.Context context) {
			System.out.println(String.format("once map oper: value=%s", value.toString()));
			String user2Friend = value.toString();
			String user = user2Friend.split(":")[0];
			String friends = user2Friend.split(":")[1];
			Arrays.asList(friends.split(",")).stream().forEach(friend -> {
				try {
					context.write(new Text(friend), new Text(user));
					System.out.println(String.format("once map write: key=%s, value=%s", friend, user));
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
			System.out.println(String.format("once reduce oper: key=%s, values=%s", key, users));

			// 用户组：[A,B,C] -> A-B,A-C,B-C 梯形，遍历嵌套
			for (int i = 0; i < users.size(); i++) {
				for (int j = i + 1; j < users.size(); j++) {
					String userCp = users.get(i) + "-" + users.get(j);
					String seqUserCp = seqUserCp(userCp);
					context.write(new Text(seqUserCp), new Text(friend));
					System.out.println(String.format("once reduce write: key=%s, value=%s", seqUserCp, friend));
				}
			}
		}

		/**
		 * @goal 对用户对进行排序，如 A-B -> A-B, B-A -> A-B 为后续操作带来便利
		 * 
		 * @param userCp
		 * @return
		 */
		private String seqUserCp(String userCp) {
			Set<String> userSet = new TreeSet<>();
			userSet.addAll(Arrays.asList(userCp.split("-")));

			StringBuilder seqUserCpCache = new StringBuilder();
			userSet.stream().forEach(user -> seqUserCpCache.append(user).append("-"));
			return seqUserCpCache.subSequence(0, seqUserCpCache.length() - 1).toString();
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");
		Job bothOneFriendJob = Job.getInstance(conf);
		bothOneFriendJob.setJarByClass(BothOneFriendMR.class);

		bothOneFriendJob.setMapperClass(BothOneFriendMapper.class);
		bothOneFriendJob.setReducerClass(BothOneFriendReducer.class);

		bothOneFriendJob.setMapOutputKeyClass(Text.class);
		bothOneFriendJob.setMapOutputValueClass(Text.class);

		bothOneFriendJob.setOutputKeyClass(Text.class);
		bothOneFriendJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(bothOneFriendJob, new Path("D:/tmp/mr/qq/data.txt"));
		FileOutputFormat.setOutputPath(bothOneFriendJob, new Path("D:/tmp/mr/qq/out_bothone"));

		boolean flag = bothOneFriendJob.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}

}
