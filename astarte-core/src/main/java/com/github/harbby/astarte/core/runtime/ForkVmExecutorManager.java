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

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ForkVmExecutorManager
        extends ExecutorManager
{
    private static final Logger logger = LoggerFactory.getLogger(ForkVmExecutorManager.class);
    private final int vcores;
    private final int memMb;
    private final int executorNum;
    private final List<VmPromise<Integer>> vms;
    private final ExecutorService pool;
    private final SocketAddress driverManagerAddress;

    public ForkVmExecutorManager(int vcores, int memMb, int executorNum, SocketAddress driverManagerAddress)
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
        //启动所有Executor
        for (int i = 0; i < executorNum; i++) {
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    //.setName("astarte.TaskExecutor")
                    .setEnvironment(Constant.DRIVER_SCHEDULER_ADDRESS, driverManagerAddress.toString())
                    .addUserJars(Platform.getSystemClassLoaderJars())
                    .setConsole(System.out::println)
                    .setXmx(memMb + "m")
                    .build();
            VmPromise<Integer> vmPromise = runJvm(launcher, vcores, executorNum);
            pool.submit(() -> {
                try {
                    vmPromise.call();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
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
        pool.shutdown();
        vms.forEach(VmPromise::cancel);
    }
}
