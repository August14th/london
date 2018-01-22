package org.hello.london.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateEvent;
import org.hello.london.db.Messages;
import org.hello.london.db.OfflineMessagesMeta;
import org.hello.london.db.Subscribes;
import org.hello.london.db.Topics;
import org.hello.london.resource.Resources;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class MqttHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static ExecutorService executor = Executors.newCachedThreadPool();

    private String userId;

    private Channel channel;

    private List<String> topics = new ArrayList<>();

    private Topics topicTable;

    private Subscribes subTable;

    private Messages msgTable;

    private Dispatcher dispatcher;

    private OnlineState state;

    private Queue<ToAck> toAcks = new LinkedBlockingQueue<>();

    private LinkedBlockingQueue<Message> buffer = new LinkedBlockingQueue<>();

    public MqttHandler(Resources resources, Dispatcher dispatcher, OnlineState state) {
        this.dispatcher = dispatcher;
        this.state = state;
        msgTable = new Messages(resources.mongo);
        topicTable = new Topics(resources.postgres, msgTable);
        subTable = new Subscribes(resources.postgres);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.channel = ctx.channel();
    }

    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        executor.execute(() -> {
            try {
                MqttMessageType type = msg.fixedHeader().messageType();
                switch (type) {
                    case CONNECT:
                        onConnect((MqttConnectMessage) msg);
                        break;
                    case PUBLISH:
                        onPublish((MqttPublishMessage) msg);
                        break;
                    case SUBSCRIBE:
                        onSubscribe((MqttSubscribeMessage) msg);
                        break;
                    case UNSUBSCRIBE:
                        onUnSubscribe((MqttUnsubscribeMessage) msg);
                        break;
                    case PUBACK:
                        onPubAck((MqttPubAckMessage) msg);
                        break;
                    case PINGREQ:
                        onPingReq();
                        break;
                    case DISCONNECT:
                        onDisconnect();
                        break;
                    default:
                        throw new RuntimeException("UnSupported message type: " + type);
                }
            } catch (Exception e) {
                e.printStackTrace();
                channel.close();
            }
        });
    }

    private void onConnect(MqttConnectMessage msg) throws Exception {
        userId = msg.payload().userName();
        String password = msg.payload().password();
        state.enter(userId);
        this.topics.addAll(subTable.get(userId));
        this.dispatcher.register(topics, this);
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        MqttConnAckVariableHeader variable = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
        System.out.println(this.userId + " Connected");
        MqttConnAckMessage ack = new MqttConnAckMessage(fixed, variable);
        this.channel.writeAndFlush(ack);
        this.sendOfflineMessages();
    }

    private void onDisconnect() {
        System.out.println("Receive disconnect message from " + this.userId);
        channel.close();
    }

    private void onSubscribe(MqttSubscribeMessage sub) throws Exception {
        List<MqttTopicSubscription> list = sub.payload().topicSubscriptions();
        List<Integer> qos = new ArrayList<>();
        List<String> topics = new ArrayList<>();
        for (MqttTopicSubscription s : list) {
            topics.add(s.topicName());
            qos.add(s.qualityOfService().value());
        }
        subTable.sub(userId, topics);
        dispatcher.register(topics, this);
        this.topics.addAll(topics);
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(sub.variableHeader().messageId());
        MqttSubAckPayload payload = new MqttSubAckPayload(qos);
        MqttSubAckMessage ack = new MqttSubAckMessage(fixed, variableHeader, payload);
        System.out.println(this.userId + " subscribed to " + topics);
        this.channel.writeAndFlush(ack);
    }

    private void onUnSubscribe(MqttUnsubscribeMessage unSub) throws Exception {
        List<String> topics = unSub.payload().topics();
        this.dispatcher.deregister(topics, this);
        this.subTable.unSub(this.userId, topics);
        this.topics.removeAll(topics);
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(unSub.variableHeader().messageId());
        MqttUnsubAckMessage ack = new MqttUnsubAckMessage(fixed, variableHeader);
        System.out.println(this.userId + " unSubscribed to " + topics);
        this.channel.writeAndFlush(ack);
    }

    private void onPublish(MqttPublishMessage publish) throws Exception {
        if (publish.fixedHeader().qosLevel() != MqttQoS.AT_LEAST_ONCE) {
            throw new RuntimeException("Unsupported Qos: " + publish.fixedHeader().qosLevel());
        }
        String topic = publish.variableHeader().topicName();
        ByteBuf byteBuf = publish.payload();
        byte[] payload = new byte[byteBuf.readableBytes()];
        byteBuf.getBytes(byteBuf.readerIndex(), payload, 0, byteBuf.readableBytes());
        topicTable.publish(topic, payload);

        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        MqttMessageIdVariableHeader variable = MqttMessageIdVariableHeader.from(publish.variableHeader().messageId());
        MqttPubAckMessage ack = new MqttPubAckMessage(fixed, variable);
        this.channel.writeAndFlush(ack);
    }

    private void onPubAck(MqttPubAckMessage ack) throws Exception {
        int ackId = ack.variableHeader().messageId();
        ToAck next = this.toAcks.poll();
        if (next != null && ackId == next.msgid) {
            this.subTable.updateLastAckId(this.userId, next.topic, ackId);
        } else {
            throw new RuntimeException("Out of order. Expected: " + (next == null ? null : next.msgid) + ", but: " + ackId);
        }
    }

    private void onPingReq() throws Exception {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessage ack = new MqttMessage(fixed);
        this.channel.writeAndFlush(ack);
    }

    private void sendOfflineMessages() {
        new Thread(() -> {
            try {
                Map<String, Long> hasSent = sendOfflineMessages0();
                drainBuffer(hasSent);
            } catch (Exception e) {
                e.printStackTrace();
                channel.close();
            }
        }).start();
    }

    private void drainBuffer(Map<String, Long> hasSent) {
        drainBuffer0(hasSent);
        synchronized (this) {
            drainBuffer0(hasSent);
            buffer = null;
        }
    }

    private void drainBuffer0(Map<String, Long> hasSent) {
        while (!buffer.isEmpty()) {
            Message msg = buffer.poll();
            if (hasSent == null) {
                directSend(msg);
            } else {
                Long last = hasSent.get(msg.topic);
                if (last == null || msg.msgId > last) {
                    directSend(msg);
                }
            }
        }
    }

    private Map<String, Long> sendOfflineMessages0() throws Exception {
        // offline messages
        List<OfflineMessagesMeta> metas = subTable.getOfflineMessageMetas(userId);
        if (!metas.isEmpty()) {
            Map<String, Long> hasSent = new HashMap<>();
            metas.forEach((meta) -> {
                List<Message> messages = msgTable.get(meta);
                messages.forEach(this::directSend);
                hasSent.put(meta.topic, meta.end);
            });
            return hasSent;
        } else {
            return null;
        }
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        this.channel.close();
    }

    void send(Message msg) {
        synchronized (this) {
            if (buffer != null) {
                buffer.add(msg);
            } else {
                directSend(msg);
            }
        }
    }

    private void directSend(Message msg) {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 2 + msg.topic.length() + 2 + msg.payload.length);
        MqttPublishVariableHeader variable = new MqttPublishVariableHeader(msg.topic, (int) msg.msgId);
        MqttPublishMessage publish = new MqttPublishMessage(fixed, variable, Unpooled.wrappedBuffer(msg.payload));
        this.toAcks.add(new ToAck((int) msg.msgId, msg.topic, Calendar.getInstance().getTime()));
        this.channel.writeAndFlush(publish);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (this.userId != null) {
            if (this.topics != null) {
                this.dispatcher.deregister(this.topics, this);
            }
            state.exit(userId);
            System.out.println(this.userId + " disconnected");
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        System.out.println("Exceeds maximum idle time");
        super.userEventTriggered(ctx, evt);
        if (evt instanceof IdleStateEvent) {
            {
                this.channel.close();
            }
        }
    }
}