# Kafka driver implementation for .NET & Core

Based on [**kafka 0.9**](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)

* dotnet core supported
* throttled asynchronous producer
* advanced consumer
    * Rebalance bashed on [Kafka Client-side Assignment Proposal](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal) 
    * Automatic offset management
* zero third-party dependency

----

## Get Packages

```bash
Install-Package Chuye.Kafka
```

may be `System.Net.Sockets, System.Threading.Thread` needed

----

## Get Started

### Option
接受 Broker 集群地址作为参数，它应以单例维护生命周期

```c
    var option = new Option("ubuntu-16:9094,ubuntu-16:9093");
```

### Client
核心 API（Metadata/Produce/Fetch/Offsets/Offset Commit/Offset Fetch）的实现者，除维护了 topic-partition-broker 元数据外是无状态的，不应被业务方直接调用；

### Producer
生产者的基本实现

```
    var producer = new Producer(option);
    //producer.UseCodec(MessageCodec.Gzip); //Snappy not support yet
    var messages = new []{ "msg#1", "msg#2"};
    producer.Send("demoTopic3", messages);
```

### ThrottledProducer
内置队列的生产者实现，每当触及数量上限或调用实例的 Dispose() 批量发送消息，视 MessageCodec 进行压缩，在 web 项目中应以 PerRequest 生命周期维护

```c
    using (var producer = new ThrottledProducer(option)) {
        //producer.UseCodec(MessageCodec.Gzip); //Snappy not support yet
        var messages = new []{ "msg#1", "msg#2"};
        producer.Send("demoTopic3", messages);
    }
```

> 注意：ThrottledProducer 可能不会立即发送消息，应该保证 ThrottledProducer 实例的 Dispose() 方法得到调用。

### Consumer
消费者的基本实现，堵塞式持续拉取消息，接受 CancellationToken 参数以进行协作式取消

```c
    var cts = new CancellationTokenSource();
    using (var consumer = new Consumer(option, "demoGroupId", "demoTopic")) {
        consumer.Initialize();
        foreach (var msg in consumer.Fetch(cts.Token)) {
            //do stuff with msg
        }
    }
```

> 注意：Consumer 使用了进度跨度与和时间间隔参数判断是否进行 offset commit，总是应该调用 CancellationTokenSource.Cancel() 以**暂停**消费、避免丢失进度；进程退出、或者 Consumer 实例的 Dispose() 方法调用后才会真正离开分组，并重新触发同组消费者的负载均衡过程。

----

## CONCERNS

**Q** 生产者如何设定消息以 gzip、snappy 压缩？  
**A** 默认不压缩，支持 gzip，见 Option.ProducerConfig.MessageCodec；消费者自动展开 gzip 压缩的消息；

**Q** 生产者如何设定消息发送的 RequiredAcks？  
**A** 默认使用 RequiredAcks = 1 确保消息得到发送，不提倡忽略服务器响应的方式，见 Option.ProducerConfig.AcknowlegeStrategy；

**Q** 消费者如何设置拉取消息的大小与等待时间？  
**A** 提供了堵塞式的 fetch 接口，见 Option.ConsumerConfig.FetchBytes，Option.ConsumerConfig.FetchMilliseconds；

**Q** 客户端获取到 TopicMetadata 后将直接调用 Zookeeper 提供的 Broker 地址，如何拦截调试？  
**A** Client 对象暴露了 RequestSending 与 ResponseReceived 事件，用户注册事件后可以修改请求的目标地址用于抓包等，示例:

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
**A** 前者是整除算法，实现细节见 *Coordinato.AssigningTopicPartitions()*，后者是简单的 partition 轮换实现；添加 TraceListener 可以查看日志输出；目前处理了 RebalanceInProgressCode，更多像 GroupCoordinatorNotAvailableCode 的处理及重试逻辑待后续加入，可扩展的实现同样在计划中。

```bash
# 1. 启动 consumer A，以分组 demoGroupId 消费拥有2个分区的 demoTopic
Consumer A Information: 0 : 17:16:04.534 [11] #1 Rebalace start at group 'demoGroupId'
Consumer A Information: 0 : 17:16:04.555 [11] #2 Group coordinate got broker http://ubuntu-16:9094/ at group 'demoGroupId'
Consumer A Information: 0 : 17:16:04.561 [11] #3 Join group 'demoGroupId', assigning topic and partitions as leader

# 1.1 由于不存在其他消费者，consumer A 将两个分区 [0, 1] 指派给自己
Consumer A Information: 0 : 17:16:04.596 [11] #4 Sync group 'demoGroupId', Member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' dispathced Topic 'demoTopic'[0,1]
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
Consumer A Information: 0 : 17:16:13.629 [09] #4 Sync group 'demoGroupId', Member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' dispathced Topic 'demoTopic'[0]
Consumer A Information: 0 : 17:16:13.629 [09] #5 Heartbeat of member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' at group 'demoGroupId'

# 4.1. consumer B 被分派的分区id 为 1
Consumer B Information: 0 : 17:16:13.631 [11] #4 Sync group 'demoGroupId', Member 'Chuye.Kafka-08516ce2-6d21-452a-8782-64d0928ddae1' dispathced Topic 'demoTopic'[1]
Consumer B Information: 0 : 17:16:13.634 [11] #5 Heartbeat of member 'Chuye.Kafka-08516ce2-6d21-452a-8782-64d0928ddae1' at group 'demoGroupId'

# 5. consumer B 离开分组
Consumer B Information: 0 : 17:16:36.621 [09] #6 Member 'Chuye.Kafka-08516ce2-6d21-452a-8782-64d0928ddae1' leave group 'demoGroupId'

# 6. consumer A 再次负载均衡
Consumer A Information: 0 : 17:16:40.663 [10] #6 Need rebalace at group 'demoGroupId' for 'RebalanceInProgressCode'
Consumer A Information: 0 : 17:16:40.663 [10] #1 Rebalace start at group 'demoGroupId'
Consumer A Information: 0 : 17:16:40.667 [10] #3 Join group 'demoGroupId', assigning topic and partitions as leader
Consumer A Information: 0 : 17:16:40.672 [10] #4 Sync group 'demoGroupId', Member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' dispathced Topic 'demoTopic'[0,1]
Consumer A Information: 0 : 17:16:40.672 [10] #5 Heartbeat of member 'Chuye.Kafka-e0418c7f-4d34-4889-a1b5-4500977e8902' at group 'demoGroupId'
```

----

### About Performance
早前在 UCLOUD 的 Kafka 集群上进行了不同连接数的吞吐量测试，见 [Kafka-cluster-testing-on-ucloud](doc/Kafka-cluster-testing-on-ucloud.md)


## LICENCE
[Apache-2.0](https://github.com/jusfr/Chuye.Kafka/blob/master/LICENSE)

### Contributing / Pull Requests
欢迎参与讨论和贡献代码，QQ 群 225437313