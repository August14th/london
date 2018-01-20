package org.hello.london.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.hello.london.resource.Resources;
import org.hello.london.util.C3P0NativeJdbcExtractor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MqttServer {

    public static void main(String[] args) throws Exception {
        Resources resources = new Resources();
        C3P0NativeJdbcExtractor c3p0NativeJdbcExtractor = new C3P0NativeJdbcExtractor();
        Dispatcher dispatcher = new Dispatcher(c3p0NativeJdbcExtractor.getNativeConnection(resources.postgres.getConnection()));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(dispatcher);

        OnlineState state = new OnlineState();

        ServerBootstrap bootstrap = new ServerBootstrap();
        EventLoopGroup worker = new NioEventLoopGroup();
        try {
            bootstrap.group(worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new MqttDecoder());
                            ch.pipeline().addLast(MqttEncoder.INSTANCE);
                            ch.pipeline().addLast(new IdleStateHandler(resources.maxIdleTime, 0, 0));
                            ch.pipeline().addLast(new MqttHandler(resources.postgres, dispatcher, resources.mongo, state));
                        }
                    });
            ChannelFuture future = bootstrap.bind(resources.port).sync();
            future.channel().closeFuture().sync();
        } finally {
            worker.shutdownGracefully();
        }
    }
}

