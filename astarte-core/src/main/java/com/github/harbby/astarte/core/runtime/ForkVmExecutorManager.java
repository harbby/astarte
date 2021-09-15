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

import com.github.harbby.astarte.core.api.Constant;
import com.github.harbby.gadtry.base.Platform;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import com.github.harbby.gadtry.jvm.VmPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ForkVmExecutorManager
        extends ExecutorManager
{
    public static final List<URL> JARS = new ArrayList<>();
    private static final Logger logger = LoggerFactory.getLogger(ForkVmExecutorManager.class);
    private final int vcores;
    private final int memMb;
    private final int executorNum;
    private final List<VmPromise<Integer>> vms;
    private final ExecutorService pool;
    private final InetSocketAddress driverManagerAddress;
    private volatile boolean shutdown = false;

    public ForkVmExecutorManager(int vcores, int memMb, int executorNum, InetSocketAddress driverManagerAddress)
    {
        super(vcores, memMb, executorNum);
        this.vcores = vcores;
        this.memMb = memMb;
        this.executorNum = executorNum;
        this.pool = Executors.newFixedThreadPool(executorNum, r -> {
            Thread thread = new Thread(r);
            thread.setName("pool_fork_jvm_" + thread.getId());
            return thread;
        });
        this.vms = new ArrayList<>(executorNum);
        this.driverManagerAddress = driverManagerAddress;
    }

    @Override
    public void start()
    {
        JVMLaunchers.VmBuilder<Integer> builder = JVMLaunchers.<Integer>newJvm();
        List<String> ops = new ArrayList<>();
        if (Platform.getJavaVersion() >= 16) {
            ops.add("--add-exports=java.base/sun.nio.ch=ALL-UNNAMED");
            ops.add("--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED");
            ops.add("--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED");
            ops.add("--add-opens=java.base/jdk.internal.loader=ALL-UNNAMED");
            ops.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        }
        ops.forEach(builder::addVmOps);
        //启动所有Executor
        for (int i = 0; i < executorNum; i++) {
            JVMLauncher<Integer> launcher = builder
                    .setName("AstarteForkVmTaskExecutor")
                    .setEnvironment(Constant.DRIVER_SCHEDULER_ADDRESS, driverManagerAddress.getHostName() + ":" + driverManagerAddress.getPort())
                    .addUserJars(JARS)
                    .setConsole(System.out::println)
                    .setXmx(memMb + "m")
                    .build();
            VmPromise<Integer> vmPromise = runJvm(launcher, vcores, executorNum);
            pool.submit(() -> {
                try {
                    vmPromise.call();
                }
                catch (Exception e) {
                    if (!shutdown) {
                        logger.error("child VM executor failed!", e);
                    }
                }
            });
            vms.add(vmPromise);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> vms.forEach(VmPromise::cancel)));
    }

    private static VmPromise<Integer> runJvm(JVMLauncher<Integer> launcher,
            int vcores, int executorNum)
    {
        return launcher.start(() -> {
            logger.info("**************fork jvm*********************");
            logger.info("starting... TaskExecutor, vcores[" + vcores + "] mem[" + executorNum + "MB]");
            TaskExecutor.main(new String[0]);
            return 0;
        });
    }

    @Override
    public void stop()
    {
        shutdown = true;
        pool.shutdown();
        vms.forEach(VmPromise::cancel);
    }
}
