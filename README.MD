# Kafka driver implementation for .NET & Core

本类库基于 **kafka 0.9** 协议，包含的实现：

* 支持 dotnet core，.net 452
* 节流、异步的 producer
* 高级的 consumer
    * 基于 GROUP API 的负载均衡实现，见 [Kafka Client-side Assignment Proposal](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal) 
    * 自动进度保存的、可配置的的 offset 管理策略
* 零第三方类库依赖

----

## Usage

```bash
Install-Package Chuye.Kafka –IncludePrerelease –version 1.0.1-beta
```

对于 dotnet core,

```json
  "dependencies": {
    "Microsoft.NETCore.App": {
      "type": "platform",
      "version": "1.0.1"
    },
    "Chuye.Kafka": "1.0.1-beta",
    "System.Diagnostics.TraceSource": "4.0.0"    
  },
```

### Option
接受 Broker 集群地址作为参数，它拥有最长的生命周期，在 web 项目中应单例维护

```c
    var option = new Option("ubuntu-16:9094,ubuntu-16:9093");
```

### Client
核心 API（Metadata/Produce/Fetch/Offsets/Offset Commit/Offset Fetch）的实现者，除维护了 topic-partition-broker 元数据外是无状态的，不应被业务方直接调用；

### Producer
生产者的基本实现

```
    var producer = new Producer(option);
    //producer.Config.MessageCodec = MessageCodec.Gzip;
    var messages = new []{ "msg#1", "msg#2"};
    producer.Send("demoTopic3", messages);
```

### ThrottledProducer
内置队列的生产者实现，每当触及数量上限及 Dispose() 时批量发送消息， 在 web 项目中应以 PerRequest 生命周期维护

```c
    using (var producer = new ThrottledProducer(option)) {
        var messages = new []{ "msg#1", "msg#2"};
        producer.Send("demoTopic3", messages);
    }
```

> 注意：ThrottledProducer 可能不会立即发送消息，应该保证 ThrottledProducer 实例的 Dispose() 方法得到调用。

### Consumer
消费者的基本实现，堵塞式持续拉取消息，接受 CancellationToken 作为参数实现了协作式取消

```c
    var cts = new CancellationTokenSource();
    using (var consumer = new Consumer(option, "demoGroupId", "demoTopic")) {
        consumer.Initialize();
        foreach (var msg in consumer.Fetch(cts.Token)) {
            //do stuff with msg
        }
    }
```

> 注意：Consumer 维护着消费进度，提交策略使用消费进度和时间间隔参数，问题应该使用 CancellationTokenSource.Cancel()　以协作式取消方式停止消费。
> 进程退出、或者 Consumer 实例的 Dispose() 方法调用后才会真正离开分组、重新触发其他消费者的负载均衡。

----

## Concerns

**Q** 生产者如何设定消息以 gzip、snappy 压缩？  
**A** 默认不压缩，支持 gzip，见 Option.ProducerConfig.MessageCodec；消费者自动展开 gzip 压缩后的消息；

**Q** 生产者如何设定消息发送的 RequiredAcks？  
**A** 默认使用 RequiredAcks = 1 确保消息得到发送，见 Option.ProducerConfig.AcknowlegeStrategy；

**Q** 消费者如何设置拉取消息的大小与等待时间？  
**A** kafka 提供了堵塞式的 fetch 接口，见 Option.ConsumerConfig.FetchBytes，Option.ConsumerConfig.FetchMilliseconds；

**Q** 消费者负载均衡状态机实现的怎么样？  
**A** 工作得不错，目前处理了 RebalanceInProgressCode 状态码，更多像 GroupCoordinatorNotAvailableCode 及 retry 逻辑需要后续加入；

**Q** 客户端获取到 TopicMetadata 后将直接调用 Zookeeper 提供的 Broker 地址，如何 hook 以调试？  
**A** Client 对象暴露了 RequestSending 与 ResponseReceived 事件供用户拦截对待发送请求和已收到的响应，修改目标地址还可以定向请求例如 Socket 抓包分析，示例:

```c
    var option = new Option("ubuntu-16:9094,ubuntu-16:9093");
    option.GetSharedClient().RequestSending += (sender, args) => {
        args.Uri = new Uri("http://localhost:9000");
        // do stuff with args.Request
    };
    option.GetSharedClient().ResponseReceived += (sender, args) => {
        //do stuff with args.Response
    };
```

**Q** 消息者负载均衡算法、生产者消息路由策略是怎样的，能否扩展？  
**A** 前者是整除算法，实现细节见 *Coordinato.AssigningTopicPartitions()*，后者是简单的 partition 轮换实现；添加 TraceListener 可以查看日志输出；可扩展的实现在计划中。

```bash
# 1. 启动 consumer A，以分组 demoGroupId 消费拥有2个分区的 demoTopic
Consumer A Information: 0 : 17:16:04.534 [11] #1 Rebalace start at group 'demoGroupId'
Consumer A Information: 0 : 17:16:04.555 [11] #2 Group coordinate got broker http://ubuntu-16:9094/ at group 'demoGroupId'
Consumer A Information: 0 : 17:16:04.561 [11] #3 Join group 'demoGroupId', assigning topic and partitions as leader

# 1.1 由于不存在其他消费者，consumer A 将两个分区 [0, 1] 指派给自己
Consumer A Information: 0 : 17:16:04.596 [11] #4 Sync group 'demoGroupId', Member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' dispathced Topic 'demoTopic'(0|1)
Consumer A Information: 0 : 17:16:04.599 [11] #5 Heartbeat of member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' at group 'demoGroupId'

# 2. 启动 consumer B, 加入 demoGroupId 试图参与消费 demoTopic
Consumer B Information: 0 : 17:16:08.808 [11] #1 Rebalace start at group 'demoGroupId'
Consumer B Information: 0 : 17:16:08.827 [11] #2 Group coordinate got broker http://ubuntu-16:9094/ at group 'demoGroupId'
Consumer B Information: 0 : 17:16:13.621 [11] #3 Join group 'demoGroupId', waiting for assingments as follower

# 3. consumer A 收到心跳响应码为 RebalanceInProgressCode，尝试重新负载均衡
Consumer A Information: 0 : 17:16:13.616 [09] #6 Need rebalace at group 'demoGroupId' for 'RebalanceInProgressCode'
Consumer A Information: 0 : 17:16:13.616 [09] #1 Rebalace start at group 'demoGroupId'

# 4. 分组 leader 被重新指派，topic-partition 被重新分派，consumer A 被分派的分区id 为 0 
Consumer A Information: 0 : 17:16:13.620 [09] #3 Join group 'demoGroupId', assigning topic and partitions as leader
Consumer A Information: 0 : 17:16:13.629 [09] #4 Sync group 'demoGroupId', Member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' dispathced Topic 'demoTopic'(0)
Consumer A Information: 0 : 17:16:13.629 [09] #5 Heartbeat of member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' at group 'demoGroupId'

# 4.1. consumer B 被分派的分区id 为 1
Consumer B Information: 0 : 17:16:13.631 [11] #4 Sync group 'demoGroupId', Member 'Chuye.Kafka-08516ce2-6d21-452a-8782-64d0928ddae1' dispathced Topic 'demoTopic'(1)
Consumer B Information: 0 : 17:16:13.634 [11] #5 Heartbeat of member 'Chuye.Kafka-08516ce2-6d21-452a-8782-64d0928ddae1' at group 'demoGroupId'

# 5. consumer B 离开分组，consumer A 再次负载均衡
Consumer B Information: 0 : 17:16:36.621 [09] #6 Member 'Chuye.Kafka-08516ce2-6d21-452a-8782-64d0928ddae1' leave group 'demoGroupId'
Consumer A Information: 0 : 17:16:40.663 [10] #6 Need rebalace at group 'demoGroupId' for 'RebalanceInProgressCode'
Consumer A Information: 0 : 17:16:40.663 [10] #1 Rebalace start at group 'demoGroupId'
Consumer A Information: 0 : 17:16:40.667 [10] #3 Join group 'demoGroupId', assigning topic and partitions as leader
Consumer A Information: 0 : 17:16:40.672 [10] #4 Sync group 'demoGroupId', Member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' dispathced Topic 'demoTopic'(0|1)
Consumer A Information: 0 : 17:16:40.672 [10] #5 Heartbeat of member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' at group 'demoGroupId'
```

----

## More

### 关于协议

Chuye.Kafka 完整实现了 Apache Kafka 0.9* 版本的协议，见 [A Guide To The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TheAPIs)。

在前置版本的实现中，序列化效率是作者的关注重点，KafkaBinary 及相关实现花费了作者大量时间，但使用中发现有收益但带来开发复杂度、增大了调试难度，另外的问题是 doetnet core 至少是起初版本缺少了 System.ServiceModel.Channels.BufferManager 相关声明——该对象是相关实现的重要环节。

出于上述顾虑及迟早迁移到 dotnet core 的想法，1.* 版本将负载均衡及 Offset 维护置于更重要的位置，在 MemoryStream 的基础上实现了协议部分的序列化与反序列化逻辑。

### 关于性能

早前在 UCLOUD 的 Kafka 集群上进行了不同连接数的吞吐量测试，见 [Kafka-cluster-testing-on-ucloud](doc/Kafka-cluster-testing-on-ucloud.md)
