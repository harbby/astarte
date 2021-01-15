package com.github.harbby.ashtarte.runtime;

import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.github.harbby.gadtry.io.IOUtils;
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
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
    private final File shuffleWorkDir;
    private ChannelFuture future;

    public ShuffleManagerService(String executorUUID)
    {
        this.shuffleWorkDir = getShuffleWorkDir(executorUUID);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            File workDir = ShuffleManagerService.getShuffleWorkDir(executorUUID);
            try {
                FileUtils.deleteDirectory(workDir);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

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

    public static byte[] testByte = new byte[] {0, 0, 0, 114, -84, -19, 0, 5, 115, 114, 0, 48,
            99, 111, 109, 46, 103, 105, 116, 104, 117, 98, 46, 104, 97, 114, 98, 98, 121, 46,
            103, 97, 100, 116, 114, 121, 46, 99, 111, 108, 108, 101, 99, 116, 105, 111, 110, 46,
            116, 117, 112, 108, 101, 46, 84, 117, 112, 108, 101, 50, -22, 124, 31, -22, -4, 89,
            46, 52, 2, 0, 2, 76, 0, 2, 102, 49, 116, 0, 18, 76, 106, 97, 118, 97, 47, 108, 97,
            110, 103, 47, 79, 98, 106, 101, 99, 116, 59, 76, 0, 2, 102, 50, 113, 0, 126, 0, 1,
            120, 112, 116, 0, 1, 97, 116, 0, 2, 97, 97, 0, 0, 0, 115, -84, -19, 0, 5, 115, 114,
            0, 48, 99, 111, 109, 46, 103, 105, 116, 104, 117, 98, 46, 104, 97, 114, 98, 98, 121,
            46, 103, 97, 100, 116, 114, 121, 46, 99, 111, 108, 108, 101, 99, 116, 105, 111, 110,
            46, 116, 117, 112, 108, 101, 46, 84, 117, 112, 108, 101, 50, -22, 124, 31, -22, -4,
            89, 46, 52, 2, 0, 2, 76, 0, 2, 102, 49, 116, 0, 18, 76, 106, 97, 118, 97, 47, 108,
            97, 110, 103, 47, 79, 98, 106, 101, 99, 116, 59, 76, 0, 2, 102, 50, 113,
            0, 126, 0, 1, 120, 112, 116, 0, 1, 98, 116, 0, 3, 98, 98, 98, -1, -1, -1, -1};

    private class ShuffleServiceHandler
            extends ChannelInboundHandlerAdapter
    {
        @Override
        public void channelActive(ChannelHandlerContext ctx)
                throws Exception
        {
            logger.info("shuffleService find client {}", ctx.channel().remoteAddress());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            ByteBuf in = (ByteBuf) msg;
            int shuffleId = in.readInt();
            int reduceId = in.readInt();
            Iterator<File> iterator = getShuffleDataInput(shuffleWorkDir, shuffleId, reduceId);
            ByteBuf byteBuf = ctx.alloc().buffer();
            if (!iterator.hasNext()) {
                byteBuf.writeInt(-1);
                ctx.writeAndFlush(byteBuf);
            }
            else {
                while (iterator.hasNext()) {
                    //todo:use 4096 byte zero copy
                    try (FileInputStream inputStream = new FileInputStream(iterator.next())) {
                        byte[] testByte1 = IOUtils.readAllBytes(inputStream);
                        byteBuf.writeBytes(testByte1);
                        ctx.channel().writeAndFlush(byteBuf);
                    }
                }
            }
            ReferenceCountUtil.release(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            logger.error("", cause);
        }
    }

    public static File getShuffleWorkDir(String executorUUID)
    {
        return new File("/tmp/ashtarte-" + executorUUID);
    }

    private static Iterator<File> getShuffleDataInput(File shuffleWorkDir, int shuffleId, int reduceId)
    {
        File[] files = new File(shuffleWorkDir, "999").listFiles();
        if (files == null) {
            return Iterators.empty();
        }
        return Stream.of(files)
                .filter(x -> x.getName().startsWith("shuffle_" + shuffleId + "_")
                        && x.getName().endsWith("_" + reduceId + ".data"))
                .iterator();
    }

    /**
     * 实验用
     */
    @Deprecated
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
                        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file))) {
                            int length = dataInputStream.readInt();
                            while (length != -1) {
                                byte[] bytes = new byte[length];
                                dataInputStream.read(bytes);
                                out.add(Serializables.byteToObject(bytes));
                                length = dataInputStream.readInt();
                            }
                        }
                        return out.iterator();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).iterator();

        return Iterators.concat(iterator);
    }
}
