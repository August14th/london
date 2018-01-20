# london
london是一个使用Netty+Postgres实现的MQTT服务器，暂时只支持QoS1。

london支持分布式部署，并且所有的MQTT终端看到的消息顺序是一致的。这得益于Postgres的Listen/Notify以及事务。

## 架构
![london](https://raw.githubusercontent.com/linyuhe/london/master/doc/london.png)

### 接收消息（Inbound）
london在收到publish消息后，会在Postgres中开启一个事务。<br/>
1、更新这条消息所属的topic的LastMessageId + 1<br/>
2、把LastMessageId作为这条消息的id, 同这条消息一同保存到MongoDB中<br/>
3、Notify到PostgresSQL的channel中。Postgres会把这消息通知到正在监听这条channel的Dispatcher<br/>
最后结束事务。<br/>

之所以在事务中更新消息Id和Notify，是因为这样可以保证正在监听的Dispatcher收到的消息的message Id是有序的，
如果update和notify不在一个事务中，那么就会出现后生成的message Id先到达Dispatcher的情况，导致消息乱序。

### 发送消息（Outbound）
Dispatcher在接收到Postgres的channel发送过来的消息后，直接扔到一个buffer中，接下来会由Sending-thread来发送给用户。
典型的生产者-消费者模型。