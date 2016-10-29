## 集群概况

* Broker 数量为4, 最低配置
* 消息格式为 "{Guid}#{index}", 由 Guid 和 For 循环当前数值以 # 拼接而成
* 消费队列缺省容量为 50 个, 达到时压缩发送

## 生产水平测试

### 2个连接

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-1.jpg)

7300+ 消息/sec.; 25w Byte/sec. 数据写入;

### 4个连接

1.4w+ 消息/sec.; 50w Byte/sec. 数据写入;

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-2.jpg)

### 32 个连接

9w+ 消息/sec.; 317w Byte/sec. 数据写入, 合约 3 MB/sec.; 

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-3.jpg)

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-4.jpg)

网卡发送水平在 30 Mbps 左右, CPU 100%;

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-5.jpg)

Zabbix 监控印证

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-6.jpg)

### 64 个连接

32 个连接时客户机 CPU 已满，另开一台客户机测试 64 个连接的水平;

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-7.jpg)

17.6w+ 消息/sec.; 618w+ Byte/sec. 数据写入, 合约 5.9 MB/sec.

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-8.jpg)

## 消费水平测试

测试期间产生数据 1y 左右, 消费水平测试准备并不充分，以下所提及时间为估算

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-9.jpg)

启动1个消费者, 独占了4个分区

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-10.jpg)

加入1个消费者, 4个分区自动平均分配

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-11.jpg)

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-12.jpg)

百w/消费/分钟的水平，流量在 10 MB/sec. 左右，

重置消费进度，使用4个消费者从头开始消费全部的1亿条，

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-13.jpg)

5分钟左右消费完，500w/消费者/分钟的水平，流量是 15 MB/sec.，

![kafka-cluster-ucloud](images/kafka-cluster-ucloud-14.jpg)

更多消费者的不再测试，因为分区无法继续分摊给多出来的消费者
