# london
london是一个使用Netty+Postgres实现的MQTT服务器，暂时只支持QoS1。

london支持分布式部署，并且所有的MQTT终端看到的消息顺序是一致的。这得益于Postgres的Listen/Notify。

# TODO
MQTT对于聊天的场景仍然不够友好，为了支持聊天，需要修改和扩展MQTT协议，主要是支持类似于HTTP协议的请求-应答的交互方式。
