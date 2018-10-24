package com.yumtao.component.outputformat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @notice 自定义OutputFormat, 重写getRecordWriter方法 。
 * @goal 当前的业务是根据不同的key，写入到不同的文件中
 * 
 * @author yumTao
 *
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new MyRecordWriter();
	}

	static class MyRecordWriter extends RecordWriter<Text, NullWritable> {

		private File localContent = new File("D:/tmp/mr/urlcontent/content.log");
		private File localToCrawler = new File("D:/tmp/mr/urlcontent/toCrawler.log");

		private BufferedWriter bWriter;

		private StringBuilder contentSb = new StringBuilder();
		private StringBuilder toCrawlerSb = new StringBuilder();

		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			if (key.toString().contains("tocrawl")) {
				toCrawlerSb.append(key.toString());
			} else {
				contentSb.append(key.toString());
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			writeToFile(contentSb.toString(), localContent);
			writeToFile(toCrawlerSb.toString(), localToCrawler);
		}

		private void writeToFile(String content, File descFile) throws IOException {
			if (StringUtils.isNotEmpty(content)) {
				bWriter = new BufferedWriter(new FileWriter(descFile));
				bWriter.write(content);
				bWriter.close();
			}
		}

	}

}
