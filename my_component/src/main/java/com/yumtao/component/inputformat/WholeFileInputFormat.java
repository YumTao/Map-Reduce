package com.yumtao.component.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @goal 自定义InputFormat。 
 * @notice 1.重写createRecordReader()方法，实现切片读取策略
 * @notice 2.重写isSplitable()方法，决定是否对文件切片
 * 
 * @author yumTao
 *
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
	// 设置每个小文件不可分片,保证一个小文件生成一个key-value键值对
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(split, context);
		return reader;
	}

	/**
	 * @goal 自定义RecordReader
	 * @notice nextKeyValue():kv读取策略。
	 * @notice getCurrentKey():获取k
	 * @notice getCurrentValue():获取v
	 * @notice getProgress():读取进度
	 * @notice initialize():初始化方法
	 * 
	 * @author yumTao
	 *
	 */
	static class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
		private FileSplit fileSplit;
		private Configuration conf;
		private BytesWritable value = new BytesWritable();
		private boolean processed = false;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			this.fileSplit = (FileSplit) split;
			this.conf = context.getConfiguration();
		}

		/**
		 * 读取切片，设置kv值，这里是读取整个小文件内容做为value，key为null
		 */
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!processed) {
				byte[] contents = new byte[(int) fileSplit.getLength()];
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(conf);
				FSDataInputStream in = null;
				try {
					in = fs.open(file);
					IOUtils.readFully(in, contents, 0, contents.length);
					value.set(contents, 0, contents.length);
				} finally {
					IOUtils.closeStream(in);
				}
				processed = true;
				return true;
			}
			return false;
		}

		/**
		 * 当前key返回null
		 */
		@Override
		public NullWritable getCurrentKey() throws IOException, InterruptedException {
			return NullWritable.get();
		}

		/**
		 * 返回value
		 */
		@Override
		public BytesWritable getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		/**
		 * 读取进程
		 */
		@Override
		public float getProgress() throws IOException {
			return processed ? 1.0f : 0.0f;
		}

		@Override
		public void close() throws IOException {
			// do nothing
		}
	}
}
