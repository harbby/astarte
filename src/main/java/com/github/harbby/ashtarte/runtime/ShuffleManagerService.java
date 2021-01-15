package com.github.harbby.ashtarte.runtime;

import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

public final class ShuffleManagerService

{
    private static final Logger logger = LoggerFactory.getLogger(ShuffleManagerService.class);

    private ChannelFuture future;

    public SocketAddress start()
            throws UnknownHostException, InterruptedException
    {
        NioEventLoopGroup boosGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boosGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel(SocketChannel ch)
                            throws Exception
                    {
                        ch.pipeline().addLast(new ShuffleServiceHandler());
                    }
                });
        this.future = serverBootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0)).sync();
        logger.info("stared shuffle service ,the port is {}", future.channel().localAddress());
        return future.channel().localAddress();
    }

    public void join()
            throws InterruptedException
    {
        future.channel().closeFuture().sync();
    }

    private static class ShuffleServiceHandler
            extends ChannelInboundHandlerAdapter
    {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            ByteBuf in = (ByteBuf) msg;
            int shuffleId = in.readInt();
            int reduceId = in.readInt();
            Iterator<FileInputStream> iterator = getShuffleDataInput(shuffleId, reduceId);
            ByteBuf byteBuf = ctx.alloc().buffer();
            while (iterator.hasNext()) {
                //todo:use 4096 byte zero copy
                byteBuf.writeBytes(iterator.next(), 20480);
                ctx.channel().writeAndFlush(byteBuf);
            }
            super.channelRead(ctx, msg);
        }
    }

    private static Iterator<FileInputStream> getShuffleDataInput(int shuffleId, int reduceId)
    {
        File dataDir = new File("/tmp/shuffle/");
        return Stream.of(dataDir.listFiles())
                .filter(x -> x.getName().startsWith("shuffle_" + shuffleId + "_")
                        && x.getName().endsWith("_" + reduceId + ".data"))
                .map(file -> {
                    try {
                        return new FileInputStream(file);
                    }
                    catch (FileNotFoundException e) {
                        throw new UncheckedIOException(e);
                    }
                }).iterator();
    }

    public static <K, V> Iterator<Tuple2<K, V>> getReader(int shuffleId, int reduceId)
    {
        File dataDir = new File("/tmp/shuffle/");
        //todo: 此处为 demo
        Iterator<Iterator<Tuple2<K, V>>> iterator = Stream.of(dataDir.listFiles())
                .filter(x -> x.getName().startsWith("shuffle_" + shuffleId + "_")
                        && x.getName().endsWith("_" + reduceId + ".data"))
                .map(file -> {
                    ArrayList<Tuple2<K, V>> out = new ArrayList<>();
                    try {
                        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file));
                        int length = dataInputStream.readInt();
                        while (length != -1) {
                            byte[] bytes = new byte[length];
                            dataInputStream.read(bytes);
                            out.add(Serializables.byteToObject(bytes));
                            length = dataInputStream.readInt();
                        }
                        dataInputStream.close();
                        return out.iterator();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).iterator();

        return Iterators.concat(iterator);
    }
}
