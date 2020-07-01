package Controller;

import Pretreatment.TableGeneTemplate;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import run.QueryInstantiator;


public class ControllerClient implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerClient.class);

    private String host = null;
    private int port;

    private Channel channel = null;

    public ControllerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void run() {
        connect();
    }

    private void connect(){
        EventLoopGroup group = new NioEventLoopGroup(1);
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        // support the native serialization of Java
                        ch.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.
                                weakCachingConcurrentResolver(this.getClass().getClassLoader())));
                        ch.pipeline().addLast(new ObjectEncoder());
                        ch.pipeline().addLast(new ControllerClientHandler());
                    }
                });
        while (true) {
            try {
                Thread.sleep(3000);
                channel = bootstrap.connect(host, port).sync().channel();
                break;
            } catch (Exception e) {
                LOGGER.error("\n\tController client startup fail!");
            }
        }
        LOGGER.debug("\n\tController client startup successful!");
    }
    public void send(TableGeneTemplate template) {
        channel.writeAndFlush(template);
    }

    public boolean isConnected() {
        return channel != null ? true : false;
    }
}
