# Kafka driver implementation for .NET
----
本类库基于 **kafka 0.9** 协议，包含的实现：

* 节流、异步的 producer
* 高级的 consumer
    * 基于 GROUP API 的负载均衡实现，见[Kafka Client-side Assignment Proposal](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal) 
    * 自动进度保存的、可配置的的 offset 管理策略

## Usage

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