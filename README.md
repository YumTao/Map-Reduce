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

### job注册提交，maptask、reducetask个数获取
		// 设置输入文件路径，根据输入文件路径，进行分片，分片个数即为maptask数。
		FileInputFormat.setInputPaths(myPartitionJob, new Path("pathurl"));
 		// 设置reducetask数，默认为1
		myPartitionJob.setNumReduceTasks(6);

### 分片
切片定义在InputFormat类中的getSplit()方法
（**默认分片策略**：先根据文件数分片，在根据blocksize分片-单个文件大于blocksize时再按blocksize进行切分，小于则不切分）

![](https://i.imgur.com/SNiUVV8.png)

### map输出内存缓冲区
内存缓冲区大小会影响到mapreduce程序的执行效率，原则上说，缓冲区越大，磁盘io的次数越少，执行速度就越快 
缓冲区的大小可以通过参数调整,  参数：io.sort.mb  默认100M

### partition
partition：对map输出按key进行分区，及排序。约定partition后的分区数=reducetask数=输出文件数。<br/>
**自定义partition**从而实现自定义的分区规则（业务场景：对key为手机号码的按归属地进行分区）<br/>
步骤：

1. 自定义类继承Partitioner抽象类，并重写getPartition()方法
1. MapReduce程序job注册指定自定义的partition类，并且设置对应reducetask数与自定义partition分区数

    	myPartitionJob.setPartitionerClass(MyPartition.class);
    	myPartitionJob.setNumReduceTasks(partitionNums);

### combine
combine：map全部写出完毕后，对缓冲区、溢出本地磁盘分区的数据进行用户自定义的程序操作，目的是为了对各分区的数据进行合并，从而缩小各分区数据的大小，进而减少reduce获取分区数据的网络传输时间。<br/>
**combine实现步骤:**<br/>


1. 自定义类继承Reducer类，重写reduce方法。注意：combine的输入与mapper的输出一致，combine的输出与reducer的输入一致。
1. MapReduce程序job注册指定combine类。
	
		myPartitionJob.setCombinerClass(MyCombiner.class);


## MR的数据压缩
mapreduce的一种优化策略：通过压缩编码对mapper或者reducer的输出进行压缩，以减少磁盘IO，提高MR程序运行速度（但相应增加了cpu运算负担）<br/>

1.	Mapreduce支持将map输出的结果或者reduce输出的结果进行压缩，以减少网络IO或最终输出数据的体积
2.	压缩特性运用得当能提高性能，但运用不当也可能降低性能
3.	基本原则：

			运算密集型的job，少用压缩
			IO密集型的job，多用压缩

###reducer输出压缩：                                                                                            
方法一: job注册类中,添加如下代码    
                                                                                 
		FileOutputFormat.setCompressOutput(job, true);                                                          
		FileOutputFormat.setOutputCompressorClass(job, Class<? extends CompressionCodec>);                      
                                                                                                        
方法二: job注册类中，设置配置：  
		                                                                                                
		conf.set("mapreduce.output.fileoutputformat.compress", "true");		                                   
		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");


## MR序列化框架

Writable（org.apache.hadoop.io.Writable）<br/>
开发背景<br/>
Java的序列化是一个重量级序列化框架（Serializable），一个对象被序列化后，会附带很多额外的信息（各种校验信息，header，继承体系。。。。），不便于在网络中高效传输；
所以，hadoop自己开发了一套序列化机制（Writable），精简，高效


###自定义对象实现MR中的序列化接口
**步骤**

1. 自定义对象，实现Writable接口（一般用WritableComparable，可实现排序）
1. 与javabean相同，添加对应set/get方法,添加有参构造的同时，需要添加对应无参构造。
1. 重写write（），readFields（）方法，注意这两个方法操作的属性**顺序要一致**。
1. 要实现排序时，重写compareTo（）方法。

**eg：**

		package com.yumtao.flowcount;
		
		import java.io.DataInput;
		import java.io.DataOutput;
		import java.io.IOException;
		
		import org.apache.hadoop.io.WritableComparable;
		
		public class FlowAndPhoneVo implements WritableComparable<FlowAndPhoneVo> {
			private long upFlow;
			private String phone;
		
			/**
			 * notice:序列化方法必须与反序列换方法顺序一致
			 */
			@Override
			public void write(DataOutput out) throws IOException {
				out.writeLong(upFlow);
				out.writeUTF(phone);
			}
		
			@Override
			public void readFields(DataInput in) throws IOException {
				upFlow = in.readLong();
				phone = in.readUTF();
			}
		
			/**
			 * order desc
			 */
			@Override
			public int compareTo(FlowAndPhoneVo o) {
				return this.upFlow > o.getUpFlow() ? -1 : 1;
			}
		
			/**
			 * notice必须给出无参构造方法
			 */
			public FlowAndPhoneVo() {
			}
		
			public FlowAndPhoneVo(long upFlow, String phone) {
				this.upFlow = upFlow;
				this.phone = phone;
			}
		
			public long getUpFlow() {
				return upFlow;
			}
		
			public void setUpFlow(long upFlow) {
				this.upFlow = upFlow;
			}
		
			public String getPhone() {
				return phone;
			}
		
			public void setPhone(String phone) {
				this.phone = phone;
			}
		
		}


##MR程序代码
[点我跳转](https://github.com/YumTao/Map-Reduce.git)
