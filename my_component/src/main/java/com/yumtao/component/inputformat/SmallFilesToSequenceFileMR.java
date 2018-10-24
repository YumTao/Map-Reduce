package com.yumtao.component.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 自定义InputFormat，实现对小文件的合并
 * @author yumTao
 *
 */
public class SmallFilesToSequenceFileMR extends Configured implements Tool {
	static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
		private Text filenameKey;

		/**
		 * 获取当前切片（自定义inputformat未对文件切片，当前切片即为当前文件）的文件名
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.toString());
		}

		@Override
		protected void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(filenameKey, value);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: combinefiles <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "combine small files to sequencefile");
		job.setJarByClass(SmallFilesToSequenceFileMR.class);

		// 设置InputFormat、OutputFormat
		job.setInputFormatClass(WholeFileInputFormat.class);
		// SequenceFileOutputFormat: 输出对象的OutputFormat
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setMapperClass(SequenceFileMapper.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[2];
		args[0] = "D:/tmp/mr/mini_file";
		args[1] = "D:/tmp/mr/mini_file/out_merge";

		int exitCode = ToolRunner.run(new SmallFilesToSequenceFileMR(), args);
		System.exit(exitCode);

	}
}
