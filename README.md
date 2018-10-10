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
- 1.根据输入文件进行分片，**分片数等于maptask数**
- 2.执行map，对应输出数据写到内存缓冲区中，超出缓冲区大小时溢写到本地磁盘。
- 3.进行partition操作，相同key在同一组，并根据key进行排序。
- 4.执行combine操作，各个maptask对分区后的各分区进行压缩，从而减小分区大小，缩短reduce获取分区数据的网络传输时间。
- 5.reducetask根据自己的分区号，到各个maptask所在机器获取对应分区，并在reducetask机器上进行分区合并、排序。
- 6.reducetask执行reduce，完成后输出文件到指定目录

### 分片
切片定义在InputFormat类中的getSplit()方法
（**分片策略**：先根据文件数分片，在根据blocksize分片-单个文件大于blocksize时再按blocksize进行切分，小于则不切分）
`此处要贴图`

### map输出内存缓冲区
内存缓冲区大小会影响到mapreduce程序的执行效率，原则上说，缓冲区越大，磁盘io的次数越少，执行速度就越快 
缓冲区的大小可以通过参数调整,  参数：io.sort.mb  默认100M

### partition
partition：对map输出按key进行分区，及排序。约定partition后的分区数=reducetask数=输出文件数
自定义partition从而实现自定义的分区规则（业务场景：对key为手机号码的按归属地进行分区）
步骤：
- 1.自定义类集成partitioner抽象类，并重写getPartition()方法
- 2.MapReduce程序job注册指定自定义的partition类，并且设置对应reducetask数与自定义partition分区数
myPartitionJob.setPartitionerClass(PhonePartition.class);
myPartitionJob.setNumReduceTasks(partitionNums);


