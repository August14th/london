package org.hello.london.core;

import com.mongodb.MongoClient;
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

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class MqttHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private String userid;

    private Channel channel;

    private List<String> topics = new ArrayList<>();

    private Topics topicTable;

    private Subscribes subTable;

    private Messages msgTable;

    private Dispatcher dispatcher;

    private OnlineState state;

    private Queue<ToAck> toAcks = new LinkedBlockingQueue<>();

    private volatile boolean ready = false;

    private Queue<Message> buffer = new LinkedBlockingQueue<>();

    public MqttHandler(DataSource postgres, Dispatcher dispatcher, MongoClient mongo, OnlineState state) {
        this.dispatcher = dispatcher;
        msgTable = new Messages(mongo);
        topicTable = new Topics(postgres);
        subTable = new Subscribes(postgres);
        this.state = state;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.channel = ctx.channel();
    }

    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        MqttMessage ack = null;
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                ack = onConnect((MqttConnectMessage) msg);
                break;
            case PUBLISH:
                ack = onPublish((MqttPublishMessage) msg);
                break;
            case SUBSCRIBE:
                ack = onSubscribe((MqttSubscribeMessage) msg);
                break;
            case UNSUBSCRIBE:
                ack = onUnSubscribe((MqttUnsubscribeMessage) msg);
                break;
            case PUBACK:
                onPubAck((MqttPubAckMessage) msg);
                break;
            case PINGREQ:
                ack = onPingReq();
                break;
            case DISCONNECT:
                ctx.channel().close();
                break;
            default:
                throw new RuntimeException("UnSupported message type: " + msg.fixedHeader().messageType());
        }
        if (ack != null) {
            ctx.channel().writeAndFlush(ack);
        }
    }

    private MqttConnAckMessage onConnect(MqttConnectMessage msg) throws Exception {
        userid = msg.payload().userName();
        String password = msg.payload().password();
        this.topics.addAll(subTable.get(userid));
        this.sendOfflineMessages();
        this.dispatcher.register(topics, this);
        state.enter(userid);
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        MqttConnAckVariableHeader variable = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
        System.out.println(this.userid + " Connected");
        return new MqttConnAckMessage(fixed, variable);
    }

    private MqttSubAckMessage onSubscribe(MqttSubscribeMessage sub) throws Exception {
        List<MqttTopicSubscription> list = sub.payload().topicSubscriptions();
        List<Integer> qos = new ArrayList<>();
        List<String> topics = new ArrayList<>();
        for (MqttTopicSubscription s : list) {
            topics.add(s.topicName());
            qos.add(s.qualityOfService().value());
        }
        subTable.sub(userid, topics);
        dispatcher.register(topics, this);
        this.topics.addAll(topics);
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(sub.variableHeader().messageId());
        MqttSubAckPayload payload = new MqttSubAckPayload(qos);
        MqttSubAckMessage ack = new MqttSubAckMessage(fixed, variableHeader, payload);
        System.out.println(this.userid + " subscribed to " + topics);
        return ack;
    }

    private MqttUnsubAckMessage onUnSubscribe(MqttUnsubscribeMessage unSub) throws Exception {
        List<String> topics = unSub.payload().topics();
        this.dispatcher.deregister(topics, this);
        this.subTable.unSub(this.userid, topics);
        this.topics.removeAll(topics);
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(unSub.variableHeader().messageId());
        MqttUnsubAckMessage ack = new MqttUnsubAckMessage(fixed, variableHeader);
        System.out.println(this.userid + " unSubscribed to " + topics);
        return ack;
    }

    private MqttPubAckMessage onPublish(MqttPublishMessage publish) throws Exception {
        if (publish.fixedHeader().qosLevel() != MqttQoS.AT_LEAST_ONCE) {
            throw new RuntimeException("Unsupported Qos: " + publish.fixedHeader().qosLevel());
        }
        String topic = publish.variableHeader().topicName();
        ByteBuf byteBuf = publish.payload();
        byte[] payload = new byte[byteBuf.readableBytes()];
        byteBuf.getBytes(byteBuf.readerIndex(), payload, 0, byteBuf.readableBytes());
        Message msg = topicTable.publish(topic, payload);
        try {
            msgTable.append(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        MqttMessageIdVariableHeader variable = MqttMessageIdVariableHeader.from(publish.variableHeader().messageId());
        return new MqttPubAckMessage(fixed, variable);
    }

    private void onPubAck(MqttPubAckMessage ack) throws Exception {
        int ackId = ack.variableHeader().messageId();
        ToAck next = this.toAcks.poll();
        if (ackId == next.msgid) {
            this.subTable.updateLastAckId(this.userid, next.topic, ackId);
        } else {
            throw new RuntimeException("Out of order. Expected: " + next + ", but: " + ackId);
        }
    }

    private MqttMessage onPingReq() throws Exception {
        MqttFixedHeader fixed = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(fixed);
    }

    private void sendOfflineMessages() throws Exception {
        List<OfflineMessagesMeta> metas = subTable.getOfflineMessageMetas(this.userid);
        for (OfflineMessagesMeta meta : metas) {
            List<Message> messages = msgTable.get(meta);
            for (Message msg : messages) {
                this.directSend(msg);
            }
        }
        while (!buffer.isEmpty()) {
            Message msg = buffer.poll();
            this.directSend(msg);
        }
        ready = true;
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.channel().close();
    }

    void send(Message msg) {
        if (ready) {
            this.directSend(msg);
        } else {
            buffer.add(msg);
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
        if (this.userid != null) {
            if (this.topics != null) {
                this.dispatcher.deregister(this.topics, this);
            }
            System.out.println(this.userid + " disconnected");
        }
        state.exit(userid);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        System.out.println("Exceeds maximum idle time");
        super.userEventTriggered(ctx, evt);
        if (evt instanceof IdleStateEvent) {
            {
                ctx.channel().close();
            }
        }
    }
}