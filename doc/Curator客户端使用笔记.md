## RetryPolicy(重试策略)
默认主要有四种实现，分别是:

* ExponentialBackoffRetry
* RetryNTimes
* RetryOneTime
* RetryUntilElapsed

# Curator客户端的使用

Curator一套开源的Zookeeper客户端框架，和ZKClient一样，Curator解决了很多Zookeeper客户端非常底层的细节开发工作，包括连接重连，反复注册Watcher和NodeExistsException异常等，目前已经成功成为了Apache的顶级项目，是全世界范围内使用最广泛的Zookeeper客户端之一。

Curator除了封装一些开发人员不需要特别关注的底层细节之外，Curator还在Zookeeper原生API的基础上进行包装，提供了一套易用性和可读性更强的Fluent风格的客户端API框架。

除此之外，Curator中还提供了Zookeeper各种应用场景（Recipe，如共享锁服务，Master选举和分布式计算器等）的抽象封装。

使用Curator需使用如下Maven依赖：
```
<dependency>
    <groupId>org.apache.curator</groupId>
    <artifactId>curator-framework</artifactId>
    <version>2.4.2</version>
</dependency>
```

## Curator常见API的使用

请参照ZkClientUtil.java类和ZkClientUtilTest.java测试类

## Curator的一些典型使用场景

Curator不仅为开发者提供了更为便利的API接口，而且还提供了一些典型场景的使用参考。读者可以从这些使用参考中更好地理解如何使用Zookeeper客户端。这些使用参考都在recipes包中，需要单独依赖以下Maven依赖来获取：
```
<dependency>
    <groupId>org.apache.curator</groupId>
    <artifactId>curator-recipes</artifactId>
    <version>2.4.2</version>
</dependency>
```

### 事件监听

Zookeeper原生支持通过注册Watcher来进行事件监听，但是其使用并不是特别方便，需要开发人员自己反复注册Watcher，比较繁琐。Curator引入了Cache来实现对Zookeeper服务端事件的监听。Cache是Curator中对事件监听的包装，其对事件的监听其实可以近似看作是一个本地缓存视图和远程Zookeeper视图的对比过程。同时Curator能够自动为开发人员处理反复注册监听，从而大大简化了原生API开发的繁琐过程。

Cache分为两类监听类型：

* 节点监听
* 子节点监听











