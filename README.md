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
