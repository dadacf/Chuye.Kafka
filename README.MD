# Kafka driver implementation for .NET & Core
----
本类库基于 **kafka 0.9** 协议，包含的实现：

* 支持 dotnet core，.net 452
* 节流、异步的 producer
* 高级的 consumer
    * 基于 GROUP API 的负载均衡实现，见[Kafka Client-side Assignment Proposal](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal) 
    * 自动进度保存的、可配置的的 offset 管理策略

## Usage

```bash
Install-Package Chuye.Kafka –IncludePrerelease –version 1.0.0-beta
```

对于 dotnet core,

```json
  "dependencies": {
    "Microsoft.NETCore.App": {
      "type": "platform",
      "version": "1.0.1"
    },
    "Chuye.Kafka": "1.0.0-beta",
    "System.Diagnostics.TraceSource": "4.0.0"    
  },
```



* Option  
接受 Broker 集群地址作为参数，它拥有最长的生命周期，在 web 项目中应单例维护

```c
    var option = new Option(new Uri("http://ubuntu-16:9094"), new Uri("http://ubuntu-16:9093"));
```

* Client  
六大核心 API（Metadata/Send/Fetch/Offsets/Offset Commit/Offset Fetch）的实现者，除维护了 topic-partition-broker 元数据外是无状态的，不应被业务方直接调用；

* Producer  
生产者的基本实现

```
    var producer = new Producer(option);
    producer.Config.MessageCodec = MessageCodec.Gzip;
    var messages = new []{ "msg#1", "msg#2"};
    producer.Send("demoTopic3", messages);
```

* ThrottledProducer  
内置队列的生产者实现，触及数量上限或 Dispose 时批量发送消息， 在 web 横在上中应以 PerRequest 生命周期维护

```c
    using (var producer = new ThrottledProducer(option)) {
        var messages = new []{ "msg#1", "msg#2"};
        producer.Send("demoTopic3", messages);
    }
```

> 注意：此模式下生产者可能不会立即发送消息，Dispose() 应该保证得到调用。

* Consumer  
消费者的基本实现，Fetch() 是堵塞式的，接受 CancellationToken 作为参数实现了协作式取消

```c
    var cts = new CancellationTokenSource();
    var consumer = new Consumer(option, "demoGroupId", "demoTopic");
    consumer.Initialize();
    foreach (var msg in consumer.Fetch(cts.Token)) {
        //do stuff with msg
    }
```

> 注意：此模式下消费者维护着消费进度，提交策略依赖了消费进度和时间间隔，CancellationTokenSource.Cancel()　是合理中止消费的方式。

## Concerns

Q 生产者如何设定消息以 gzip、snappy 压缩？  
A 默认不压缩消息，支持 gzip 形式压缩，见 Option.ProducerConfig.MessageCodec；消费者自动解压 gzip 后的消息；

Q 生产者如何设定消息发送的 RequiredAcks？  
A 默认使用 RequiredAcks = 1 确保消息得到发送，Option.ProducerConfig.AcknowlegeStrategy；

Q 消费者如何设置拉取消息的大小与等待时间？
A kafka 提供了堵塞式 fetch 接口，见 Option.ConsumerConfig.FetchBytes，Option.ConsumerConfig.FetchMilliseconds；

Q 消费者负载均衡状态机实现的怎么样？
A 能工作，目前处理了 RebalanceInProgressCode 状态码，更多像 GroupCoordinatorNotAvailableCode 及 retry 逻辑需要后续加入；

Q 消息者负载均衡算法、生产者消息路由策略是怎样的，能否扩展
A 前者是整除算法，实现细节见 *Coordinato.AssigningTopicPartitions()*，后者是简单的 partition 轮换实现；可扩展的实现在计划中.

Q 客户端获取到 TopicMetadata 后将更新 Broker 目标而不使用初始参数，如何 hook 以调试？  
A Client 对象暴露了 RequestSending 与 ResponseReceived 事件，用户可以对它们进行拦截，在进行 Socket 抓包分析，示例:

```c
    var option = new Option(new Uri("http://ubuntu-16:9094"), new Uri("http://ubuntu-16:9093"));
    option.GetSharedClient().RequestSending += (_, e) => {
        e.Uri = new Uri(e.Uri.AbsoluteUri.Replace("ubuntu-16", "localhost"));
        // do stuff with e.Request
    };
    option.GetSharedClient().ResponseReceived += (_, e) => {
        //do stuff with e.Response
    };
```

## More

### 关于协议部分

Chuye.Kafka 完整实现了 Apache Kafka 0.9* 版本的协议，见[A Guide To The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TheAPIs)。

在前置版本的实现中，序列化效率是作者的关注重点，KafkaBinary 及相关实现花费了作者大量时间，但使用中发现有收益但带来开发复杂度、增大了调试难度，另外的问题是 doetnet core 至少是起初版本缺少了 System.ServiceModel.Channels.BufferManager 相关声明——该对象是相关实现的重要环节。

出于上述顾虑及迟早迁移到 dotnet core 的想法，1.* 版本将负载均衡及 Offset 维护置于更重要的位置，在 MemoryStream 的基础上实现了协议部分的序列化与反序列化逻辑。

### 关于性能

早前在 UCLOUD 的 Kafka 集群上进行了不同连接数的吞吐量测试，见[Kafka-cluster-testing-on-ucloud](doc/Kafka-cluster-testing-on-ucloud.md)