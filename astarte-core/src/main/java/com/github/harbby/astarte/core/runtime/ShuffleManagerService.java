/*
 * Copyright (C) 2018 The Astarte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.astarte.core.runtime;

import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.EncoderInputStream;
import com.github.harbby.astarte.core.coders.Tuple2Encoder;
import com.github.harbby.gadtry.base.Files;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.github.harbby.gadtry.io.LimitInputStream;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ReferenceCountUtil;
import net.jpountz.lz4.LZ4BlockInputStream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.astarte.core.operator.SortShuffleWriter.OBJECT_COMPARATOR;
import static com.github.harbby.gadtry.base.MoreObjects.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ShuffleManagerService
{
    private static final Logger logger = LoggerFactory.getLogger(ShuffleManagerService.class);
    private final File shuffleBaseDir;
    private ChannelFuture future;
    private volatile int currentJobId;

    public ShuffleManagerService(File shuffleBaseDir)
    {
        this.shuffleBaseDir = shuffleBaseDir;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                FileUtils.deleteDirectory(shuffleBaseDir);
                logger.debug("clear shuffle data temp dir {}", shuffleBaseDir);
            }
            catch (IOException e) {
                logger.error("clear shuffle data temp dir failed", e);
            }
        }));
    }

    public void updateCurrentJobId(int jobId)
    {
        this.currentJobId = jobId;
    }

    public SocketAddress start()
            throws UnknownHostException, InterruptedException
    {
        final NioEventLoopGroup boosGroup = new NioEventLoopGroup();
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup();
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
        future.channel().closeFuture().addListener((ChannelFutureListener) channelFuture -> {
            boosGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        });
        return future.channel().localAddress();
    }

    public void join()
            throws InterruptedException
    {
        future.channel().closeFuture().sync();
    }

    public void stop()
    {
        future.channel().close();
    }

    private class ShuffleServiceHandler
            extends ChannelInboundHandlerAdapter
    {
        @Override
        public void channelActive(ChannelHandlerContext ctx)
                throws Exception
        {
            logger.info("found shuffle client {}", ctx.channel().remoteAddress());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            ByteBuf in = (ByteBuf) msg;
            int shuffleId = in.readInt();
            int reduceId = in.readInt();
            ReferenceCountUtil.release(msg);
            List<File> files = getShuffleDataInput(shuffleBaseDir, currentJobId, shuffleId, reduceId);  //获取多个待发送文件

            if (files.isEmpty()) {
                //not found any file
                ByteBuf finish = ctx.alloc().directBuffer(4, 4).writeInt(0);
                ctx.writeAndFlush(finish);
                return;
            }
            //write header info
            int headerSize = files.size() * 8 + 4;
            ByteBuf header = ctx.alloc().directBuffer(headerSize);
            header.writeInt(files.size());
            for (File file : files) {
                header.writeLong(file.length());
            }
            ctx.write(header);
            //write data by zero copy
            for (File file : files) {
                FileInputStream inputStream = new FileInputStream(file);
                ctx.writeAndFlush(new DefaultFileRegion(inputStream.getChannel(), 0, file.length()), ctx.newProgressivePromise())
                        .addListener((ChannelFutureListener) future -> {
                            logger.debug("send file {} done, size = {}", file, file.length());
                            inputStream.close();
                        });
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            logger.error("", cause);
        }
    }

    public <K, V> Iterator<Tuple2<K, V>> getShuffleDataIterator(Encoder<Tuple2<K, V>> encoder, int shuffleId, int reduceId)
            throws IOException
    {
        requireNonNull(encoder, "encoder is null");
        checkArgument(reduceId >= 0);
        List<File> files = Files.listFiles(new File(shuffleBaseDir, String.valueOf(currentJobId)), false,
                file -> file.getName().startsWith("shuffle_merged_" + shuffleId + "_"));
        List<Iterator<Tuple2<K, V>>> iterators = new ArrayList<>(files.size());
        for (File file : files) {
            //read header
            FileInputStream fileInputStream = new FileInputStream(file);
            DataInputStream dataInputStream = new DataInputStream(fileInputStream);
            long[] segmentEnds = new long[dataInputStream.readInt()];
            for (int i = 0; i < segmentEnds.length; i++) {
                segmentEnds[i] = dataInputStream.readLong();
            }
            long segmentEnd = segmentEnds[reduceId];
            long length = segmentEnd;
            if (reduceId > 0) {
                int headerSize = Integer.BYTES + segmentEnds.length * Long.BYTES;
                fileInputStream.getChannel().position(headerSize + segmentEnds[reduceId - 1]);
                length = segmentEnd - segmentEnds[reduceId - 1];
            }
            if (length > 0) {
                iterators.add(new EncoderInputStream<>(new BufferedInputStream(new LZ4BlockInputStream(new LimitInputStream(fileInputStream, length))), encoder));
            }
        }
        Comparator<K> comparator;
        if (encoder instanceof Tuple2Encoder) {
            comparator = ((Tuple2Encoder<K, V>) encoder).getKeyEncoder().comparator();
        }
        else {
            //any type 排序...
            comparator = (Comparator<K>) OBJECT_COMPARATOR;
        }
        return Iterators.mergeSorted((x, y) -> comparator.compare(x.f1, y.f1), iterators);
    }

    /**
     * hash shuffle temp file scan
     *
     * @param jobId          job id
     * @param shuffleId      shuffle id(stage id)
     * @param reduceId       reduce partition id
     * @param shuffleWorkDir shuffle temp dir
     * @return find files when reduceId
     */
    private static List<File> getShuffleDataInput(File shuffleWorkDir, int jobId, int shuffleId, int reduceId)
    {
        File[] files = new File(shuffleWorkDir, String.valueOf(jobId)).listFiles();
        if (files == null) {
            return Collections.emptyList();
        }
        return Stream.of(files)
                .filter(x -> x.getName().startsWith("shuffle_" + shuffleId + "_")
                        && x.getName().endsWith("_" + reduceId + ".data"))
                .collect(Collectors.toList());
    }
}
