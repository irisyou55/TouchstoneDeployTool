package Controller;

import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import run.QueryInstantiator;


public class ControllerServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerServerHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        @SuppressWarnings("unchecked")
        Map<Integer, ArrayList<long[]>> pkJoinInfo = (Map<Integer, ArrayList<long[]>>)msg;
        Controller.receivePkJoinInfo(pkJoinInfo);

        LOGGER.info("\n\tController receives a 'pkJoinInfo' from a data generator!");
        String response = "It's a response of the controller: I has received a 'pkJoinInfo' from you!";
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
