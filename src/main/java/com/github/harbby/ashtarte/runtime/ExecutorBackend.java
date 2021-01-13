package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.MapTaskState;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.gadtry.base.Serializables;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;

/**
 * 发送task结束信号给driver
 * 需要启用发送心跳能力
 */
public class ExecutorBackend
        extends LengthFieldBasedFrameDecoder
{
    private ChannelHandlerContext ctx;
    private final Executor executor;

    public ExecutorBackend(Executor executor)
    {
        super(6553600, 0, 4);
        this.executor = executor;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in)
            throws Exception
    {
        in = (ByteBuf) super.decode(ctx, in);
        if (in == null) {
            return null;
        }

        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readBytes(bytes);
        Task<MapTaskState> task = Serializables.byteToObject(bytes);
        executor.runTask(task);
        return task;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
            throws Exception
    {
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    public void updateState(TaskEvent event)
    {
        ByteBuf buffer = ctx.alloc().buffer();
        byte[] bytes;
        try {
            bytes = Serializables.serialize(event);
            buffer.writeInt(bytes.length).writeBytes(bytes);
            ctx.writeAndFlush(buffer);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        //throw new UnsupportedOperationException("123123123");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception
    {
        cause.printStackTrace();
    }

    /**
     * 心跳
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
            throws Exception
    {
        super.userEventTriggered(ctx, evt);
    }
}
