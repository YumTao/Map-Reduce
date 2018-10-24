# Map-Reduce

## 编程规范

- 用户编写的程序分成三个部分：Mapper，Reducer，Driver(提交运行mr程序的客户端)
- Mapper的输入数据是KV对的形式（KV的类型可自定义）
- Mapper的输出数据是KV对的形式（KV的类型可自定义）
- Mapper中的业务逻辑写在map()方法中
- map()方法（maptask进程）对每一个<K,V>调用一次
- Reducer的输入数据类型对应Mapper的输出数据类型，也是KV
- Reducer的业务逻辑写在reduce()方法中
- Reducetask进程对每一组相同k的<k,v>组调用一次reduce()方法
- 用户自定义的Mapper和Reducer都要继承各自的父类
- 整个程序需要一个Drvier来进行提交，提交的是一个描述了各种必要信息的job对象

## MapReduce介绍：
MapReduce分布式计算框架，用户只需编写具体业务代码即可。其他任务分片、数据传输、分区合并等工作全部由MapReduce框架完成。极大的减少了分布式计算程序的实现。

## MapReduce组成：
- 1、MRAppMaster：负责整个程序的过程调度及状态协调
- 2、mapTask：负责map阶段的整个数据处理流程
- 3、reduceTask：负责reduce阶段的整个数据处理流程

## MapReduce工作流程：
- 1.job提交阶段，根据driver的代码设置，决定maptask数及reducetask数。
- 2.maptask启动，InputFormat组件及其成员RecordReader读取当前分片信息,获取kv对，传输给map。
- 3.执行map，对应输出数据写到内存缓冲区中，超出缓冲区大小时溢写到本地磁盘。
- 4.进行partition操作，相同key在同一组，并根据key进行排序(map每往缓冲区写一次，就执行一次partition)。
- 5.map全部写完后，执行combine操作，各个maptask对分区后的各分区进行压缩，从而减小分区大小，缩短reduce获取分区数据的网络传输时间。
- 6.合并：内存缓冲区的数据溢写到磁盘，与之前溢写到磁盘的文件进行按分区合并，并继续执行combine，进一步减少reduce获取数据的网络传输时间。
- 7.reducetask根据自己的分区号，到各个maptask所在机器获取对应分区，并在reducetask机器上进行分区合并、排序。
- 8.reducetask执行reduce，完成后输出文件到指定目录

## 待做工作
3. 分布式缓存实现
### mapreduce其他补充：计数器应用，多job串联

	**计数器使用**

		mapper.map/reducer.reduce对应的context对象可创建共有的计数器
		//对枚举定义的自定义计数器加1
		context.getCounter(MyCounter.MALFORORMED).increment(1);
		//通过动态设置自定义计数器加1
		context.getCounter("counterGroupa", "countera").increment(1);


	**多job串联**
	
		ControlledJob cjob1 = new ControlledJob(job1.getConfiguration());
		ControlledJob cjob2 = new ControlledJob(job2.getConfiguration());

		// set dependency 设置作业依赖关系:job2依赖job1,依赖多个继续执行此API
		cjob2.addDependingJob(cjob1);

		JobControl jobControl = new JobControl("any name");
		jobControl.addJob(cjob1);
		jobControl.addJob(cjob2);
		cjob1.setJob(job1);
		cjob2.setJob(job2);

		// 新建一个线程来运行已加入JobControl中的作业，开始进程并等待结束
		Thread thread = new Thread(jobControl);
		thread.start();
		while (!jobControl.allFinished()) {
			Thread.sleep(500);
		}
		jobControl.stop();

5. mapreduce参数优化：资源相关参数，容错相关参数，本地运行mapreduce作业，效率和稳定性相关参数

MapReduce重要配置参数

**资源相关参数**

| 参数 | 参数描述 | 补充 |
| :------| :------ | :------: |
| mapreduce.map.memory.mb | 一个Map Task可使用的资源上限（单位:MB），默认为1024。 | 如果Map Task实际使用的资源量超过该值，则会被强制杀死。 |
| mapreduce.reduce.memory.mb | 一个Reduce Task可使用的资源上限（单位:MB），默认为1024。 | 如果Reduce Task实际使用的资源量超过该值，则会被强制杀死。 |
| mapreduce.map.java.opts| Map Task的JVM参数，你可以在此配置默认的java heap size等参数| "-Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc" （@taskid@会被Hadoop框架自动换为相应的taskid）, 默认值: ""|
| mapreduce.reduce.java.opts| Reduce Task的JVM参数，你可以在此配置默认的java heap size等参数| "-Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc", 默认值: ""|
| mapreduce.map.cpu.vcores| 每个Map task可使用的最多cpu core数目, 默认值: 1| 无|
| mapreduce.reduce.cpu.vcores| 每个Reduce task可使用的最多cpu core数目, 默认值: 1| 无|

**容错相关参数**

| 参数 | 参数描述 | 补充 |
| :------| :------ | :------: |
| mapreduce.map.maxattempts | 每个Map Task最大重试次数，默认值：4。 | 一旦重试参数超过该值，则认为Map Task运行失败 |
| mapreduce.reduce.maxattempts | 每个Reduce Task最大重试次数，默认为4。 | 一旦重试参数超过该值，则认为Reduce Task运行失败 |
| mapreduce.map.failures.maxpercent| 当失败的Map Task失败比例超过该值为，整个作业则失败，默认值为0| 如果你的应用程序允许丢弃部分输入数据，则该该值设为一个大于0的值，比如5，表示如果有低于5%的Map Task失败（如果一个Map Task重试次数超过mapreduce.map.maxattempts，则认为这个Map Task失败，其对应的输入数据将不会产生任何结果），整个作业扔认为成功。|
| mapreduce.reduce.failures.maxpercent| 当失败的Reduce Task失败比例超过该值为，整个作业则失败，默认值为0.| 同上|
| mapreduce.task.timeout| Task超时时间，默认是300000（单位毫秒）| 如果程序对每条输入数据的处理时间过长（比如会访问数据库，通过网络拉取数据等），建议将该参数调大|


**效率和稳定性相关参数**

| 参数 | 参数描述 | 补充 |
| :------| :------ | :------: |
| mapreduce.map.speculative| 是否为Map Task打开推测执行机制,默认为false| 无|
| mapreduce.reduce.speculative| 是否为Reduce Task打开推测执行机制,默认为false| 无|
| mapreduce.job.user.classpath.first & mapreduce.task.classpath.user.precedence| 是否有限使用用户jar包中的class| 当同一个class同时出现在用户jar包和hadoop jar中时，优先使用哪个jar包中的class，默认为false，表示优先使用hadoop jar中的class|

## MapReduce并行度经验之谈

###FileInputFormat切片机制

 切片主要由这几个值来运算决定

- minsize：默认值：1  
配置参数： **mapreduce.input.fileinputformat.split.minsize**
- maxsize：默认值：Long.MAXValue  
配置参数：**mapreduce.input.fileinputformat.split.maxsize**
- blocksize

切片大小计算公式：
		
		Math.max(minSize, Math.min(maxSize, blockSize));

**结论:**

- 默认情况下，切片大小=blocksize
- maxsize（切片最大值）：
参数如果调得比blocksize小，则会让切片变小，而且就等于配置的这个参数的值
- minsize （切片最小值）：
参数调的比blockSize大，则可以让切片变得比blocksize还大


选择并发数的影响因素：
1、运算节点的硬件配置
2、运算任务的类型：CPU密集型还是IO密集型
3、运算任务的数据量

###maptask

如果硬件配置为`2*12core + 64G`，恰当的map并行度是大约每个节点20-100个map，**最好每个map的执行时间至少一分钟**。

- 如果job的每个map或者 reduce task的运行时间都只有30-40秒钟，那么就减少该job的map或者reduce数，每一个task(map|reduce)的setup和加入到调度器中进行调度，这个中间的过程可能都要花费几秒钟，所以如果每个task都非常快就跑完了，就会在task的开始和结束的时候浪费太多的时间。
配置task的JVM重用可以改善该问题：
（mapred.job.reuse.jvm.num.tasks，默认是1，表示一个JVM上最多可以顺序执行的task
数目（属于同一个Job）是1。也就是说一个task启一个JVM）

- 如果input的文件非常的大，比如1TB，可以考虑将hdfs上的每个block size设大，比如设成256MB或者512MB

###reducetask

reducetask数由代码手动设置`job.setNumReduceTasks(num)`，默认为1;

根据具体业务需求来决定reducetask数，如果数据分布不均匀，就有可能在reduce阶段产生数据倾斜
注意： 有些情况下，需要计算全局汇总结果，就只能有1个reducetask

**约定reducetask数=partition分区数=输出文件数**

尽量不要运行太多的reduce task。对大多数job来说，最好rduce的个数最多和集群中的reduce持平，或者比集群的 reduce slots小。这个对于小集群而言，尤其重要。


###自定义inputFormat实现

目的：

- 可实现自定义切片规则
- 可实现自定义分片kv读取策略

步骤：

1. 自定义class继承InputFormat抽象类（一般实现FileInputFormat）
2. 自定义切片规则时重写getSplits()方法
3. 自定义分片kv读取策略时，重写createRecordReader(),构造自定义的RecordReader对象。
4. MR程序注册job，指定自定义InputFormat

		job.setInputFormatClass(MyInputFormat.class);

**自定义的RecordReader实现分片kv读取策略**

步骤

- 自定义class继承RecordReader<k,v>, 对应kv的泛型与目标输出的kv一致
- 重写nextKeyValue(), 编写kv读取策略,设置对应kv值
- 重写getCurrentKey(), 获取k值
- 重写getCurrentValue(), 获取v值
- 重写getProgress(), 编写分片读取进度

**eg:** 

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

### 自定义outputFormat实现
目的：指定reduce后内容写出策略

步骤：

- 自定义class继承OutputFormat(一般继承FileOutputFormat)
- 重写getRecordWriter()方法，获取自定义的RecordWriter对象
- MR程序job注册，指定自定义的OutputFormat类型

		job.setOutputFormatClass(MyOutputFormat.class);

**自定义RecordWriter实现内容写出策略**

步骤：

- 自定义class继承RecordWriter
- 重写write()方法，编写自己的写出策略

**eg**

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
	
		/**
		 * write():reduce的context.write一次，就调用一次
		 * close():全部写出后，调用一次，一般用作关流
		 * @author yumTao
		 *
		 */
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


### 自定义Partitioner

**步骤：**

1. 自定义类继承Partitioner抽象类，并重写getPartition()方法, **注意:返回值应从0开始**。
2. MapReduce程序job注册指定自定义的partition类，并且设置对应reducetask数与自定义partition分区数

		myPartitionJob.setPartitionerClass(MyPartition.class);
		myPartitionJob.setNumReduceTasks(partitionNums);

自定义Partitioner实例

	public class PhonePartition extends Partitioner<FlowAndPhoneVo, Text> {

		static Map<String, Integer> provinceMap = new HashMap<String, Integer>();
	
		static {
			provinceMap.put("135", 0);
			provinceMap.put("136", 1);
			provinceMap.put("137", 2);
			provinceMap.put("138", 3);
			provinceMap.put("139", 4);
		}
	
		@Override
		public int getPartition(FlowAndPhoneVo key, Text value, int numPartitions) {
			System.out.println(String.format("once partition oper: key=%s", key.toString()));
			String phoneHead = key.getPhone().substring(0, 3);
			Integer code = provinceMap.get(phoneHead);
			return code == null ? 5 : code.intValue();
		}

	}

### 自定义GroupingComparator

**步骤**

1. 继承WritableComparator接口 
2. 无参构造，传入对应Mapper的输出key
3. 重写compare(WritableComparable a, WritableComparable b)方法，来决定是否在同一组

自定义GroupingComparator实例

		public class OrderIdGroupingComparator extends WritableComparator {
		
			public OrderIdGroupingComparator() {
				super(OrderDetailVo.class, true);
			}
		
			/**
			 * 根据订单编号来决定是否在同一组
			 */
			@SuppressWarnings("rawtypes")
			@Override
			public int compare(WritableComparable a, WritableComparable b) {
				OrderDetailVo left = (OrderDetailVo) a;
				OrderDetailVo right = (OrderDetailVo) b;
				System.out.println(String.format("grouping compare left=%s, right=%s", left.toString(), right.toString()));
				return left.getOrderId().compareTo(right.getOrderId());
			}
		
		}


**注意：reduce读取数据时，预期同一组的数据在行数上必须连续，这样才能保证分组正常**

这是因为reduce执行<k,v>分组时，是对所在分区，分别读取两次数据，对当前两次数据进行比对，如在同一组则继续向下取一个，并判断是否在同一组，直至获取到不同组的为止，进而执行reduce（）方法业务
而默认的读取方法是按行读取。 
        
		eg : {1.1,2.1,3.2,4.1} 格式:(index.group)
		-> (1.1,2.1):同一组，继续
		-> (2.1,3.2)：不同组，将获取的同组结果[1.1,2.1]传给reduce（）执行具体业务。
		-> (3.2, 4.1)不同组，将获取的同组结果[3.2]传给reduce（）执行具体业务。
		-> 只剩下最后一个4.1，此时无需执行分组，默认自成一组传给reduce（）执行具体业务。


文件：04_离线计算系统第4天