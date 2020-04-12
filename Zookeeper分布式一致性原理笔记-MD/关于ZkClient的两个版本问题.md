关于ZkClient的两个版本问题

摘自：https://blog.csdn.net/wo541075754/article/details/68929512

ZkClient是由Datameer的工程师开发的开源客户端，对Zookeeper的原生API进行了包装，实现了超时重连、Watcher反复注册等功能。


ZkClient目前有两个不同artifactId的系列，其中最早的0.1版本maven依赖如下：

```xml
<dependency>
     <groupId>org.apache.zookeeper</groupId>
     <artifactId>zookeeper</artifactId>
     <version>3.4.9</version>
 </dependency>
 <dependency>
     <groupId>com.github.sgroschupf</groupId>
     <artifactId>zkclient</artifactId>
     <version>0.1</version>
 </dependency>
```

另外一个系列为的maven依赖为：

```xml
<dependency>
     <groupId>org.apache.zookeeper</groupId>
     <artifactId>zookeeper</artifactId>
     <version>3.4.9</version>
 </dependency>
<dependency>
    <groupId>com.101tec</groupId>
    <artifactId>zkclient</artifactId>
    <version>0.10</version>
</dependency>
```

其中第二个系列包含了从0.1~0.10的版本。查看dubbo的源代码，我们可以看到，dubbo采用了第一个系列的0.1版本。

github源代码地址：https://github.com/sgroschupf/zkclient