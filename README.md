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

文件：04_离线计算系统第4天